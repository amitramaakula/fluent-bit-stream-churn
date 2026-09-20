#!/usr/bin/env python3
"""Bounded real-Pod validation. Requires an existing, disposable Kind cluster.

Creates only one tagged, one-day-retention CloudWatch group. Deletes it and its
Kubernetes namespace in finally; cluster lifecycle is deliberately separate.
Credentials travel through kubectl stdin and are never written to artifacts.
"""
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import threading
import time
import traceback

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[1]
NS = "cwlogs-rollout"
COUNT = 32
RECORDS = 20
GENERATIONS = 3

SERVER = '''from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import json
class Handler(BaseHTTPRequestHandler):
 def do_GET(self):
  body=Path('/credentials/creds.json').read_bytes()
  value=json.loads(body)
  assert all(value.get(k) for k in ['AccessKeyId','SecretAccessKey','Token','Expiration'])
  self.send_response(200)
  self.send_header('Content-Type','application/json')
  self.send_header('Content-Length',str(len(body)))
  self.end_headers()
  self.wfile.write(body)
 def log_message(self,*args): pass
ThreadingHTTPServer(('127.0.0.1',8181),Handler).serve_forever()
'''
APP = '''import json,os,time
for i in range(20):
 print(json.dumps({'record_id':os.environ['POD_UID']+':'+str(i),
 'source_uid':os.environ['POD_UID'],'source_name':os.environ['POD_NAME'],
 'generation':os.environ['GENERATION'],'sequence':i,
 'created_ms':int(time.time()*1000),'message':'real container stdout'}),flush=True)
 time.sleep(.05)
time.sleep(3600)
'''
LUA = '''function partition(tag, timestamp, record)
 local k = record['kubernetes']
 if k == nil or k['pod_name'] == nil then return -1, timestamp, record end
 local name = k['pod_name']
 local hash = 0
 for i = 1, #name do hash = (hash * 31 + string.byte(name, i)) % 32 end
 record['bounded_stream'] = string.format('reused-logs-%02d', hash)
 record['cluster_name'] = 'local-rollout-demo'
 return 1, timestamp, record
end
'''


def configuration(region, group):
    base = '''[SERVICE]
    Flush 1
    Log_Level info
    Parsers_File /fluent-bit/etc/parsers.conf
    HTTP_Server On
    HTTP_Listen 0.0.0.0
    HTTP_Port 2020
    storage.path /buffers
    scheduler.base 2
    scheduler.cap 10
[INPUT]
    Name tail
    Path /var/log/containers/checkout-*_cwlogs-rollout_app-*.log
    Tag kube.*
    multiline.parser cri
    Read_from_Head On
    Refresh_Interval 1
    DB /buffers/tail.db
    storage.type filesystem
[FILTER]
    Name kubernetes
    Match kube.*
    Kube_Tag_Prefix kube.var.log.containers.
    Merge_Log On
    Keep_Log Off
    Labels Off
    Annotations Off
[FILTER]
    Name lua
    Match kube.*
    script /config/partition.lua
    call partition
'''
    for template, fallback in [("pod-$kubernetes['pod_name']", "ERROR-pod-fallback"),
                               ("$bounded_stream", "ERROR-reused-fallback")]:
        base += f'''[OUTPUT]
    Name cloudwatch_logs
    Match kube.*
    region {region}
    log_group_name {group}
    log_stream_name {fallback}
    log_stream_template {template}
    auto_create_group false
    workers 2
    Retry_Limit False
    storage.total_limit_size 64M
'''
    return base


def audit(events, expected):
    sets = {"pod": set(), "reused": set()}
    counts = {"pod": 0, "reused": 0}
    streams = {"pod": set(), "reused": set()}
    errors = []
    for event in events:
        name = event['logStreamName']
        design = 'reused' if name.startswith('reused-logs-') else 'pod'
        record = json.loads(event['message'])
        rid = record.get('record_id')
        metadata = record.get('kubernetes', {})
        if rid not in expected:
            errors.append(f'unexpected record {rid}')
            continue
        source = expected[rid]
        if metadata.get('pod_id') != source['uid'] or metadata.get('pod_name') != source['name']:
            errors.append(f'metadata mismatch {rid}')
        if record.get('source_uid') != source['uid']:
            errors.append(f'payload mismatch {rid}')
        if design == 'pod' and name != 'pod-' + source['name']:
            errors.append(f'stream mismatch {name}')
        if design == 'reused' and name not in {f'reused-logs-{i:02d}' for i in range(32)}:
            errors.append(f'unbounded stream {name}')
        sets[design].add(rid)
        counts[design] += 1
        streams[design].add(name)
    return {'expected_per_design': len(expected), 'metadata_errors': errors,
            'designs': {d: {'unique_records': len(sets[d]), 'delivered_records': counts[d],
                            'extra_deliveries': counts[d] - len(sets[d]),
                            'missing_records': len(set(expected) - sets[d]),
                            'streams': sorted(streams[d])} for d in sets}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cluster', default='cwlogs-rollout')
    parser.add_argument('--region', default='us-west-2')
    parser.add_argument('--account', required=True, help='Expected AWS account; refuses a mismatch')
    args = parser.parse_args()
    run = 'rollout-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = ROOT / 'results' / run
    out.mkdir(parents=True)
    group = '/kubecon/stream-churn/' + run
    session = boto3.Session(region_name=args.region)
    aws = session.client('logs', config=Config(retries={'mode': 'standard', 'max_attempts': 4}))
    stop = threading.Event()
    events, expected, cleanup = {}, {}, {}
    owned_group = owned_ns = False
    refresher = None

    def save(name, value):
        (out / name).write_text(json.dumps(value, indent=2, default=str))

    def log(message, **values):
        row = {'time': dt.datetime.now(dt.timezone.utc).isoformat(), 'event': message, **values}
        print(json.dumps(row), flush=True)
        with (out / 'timeline.jsonl').open('a') as file:
            file.write(json.dumps(row) + '\n')

    def k(*commands, data=None, timeout=180):
        result = subprocess.run(['kubectl', '--context', 'kind-' + args.cluster, *commands],
                                input=data, text=True, capture_output=True, timeout=timeout)
        if result.returncode:
            # Never echo input: Secret credentials are applied through stdin.
            raise RuntimeError(f'kubectl {commands[:2]} failed: {result.stderr}')
        return result.stdout

    def apply(resource):
        return k('apply', '-f', '-', data=json.dumps(resource))

    def refresh():
        provider = session.get_credentials()
        creds = provider.get_frozen_credentials()
        expiry = getattr(provider, '_expiry_time', None)
        if not creds.token or not expiry:
            raise RuntimeError('This experiment requires temporary AWS credentials')
        payload = {'AccessKeyId': creds.access_key, 'SecretAccessKey': creds.secret_key,
                   'Token': creds.token, 'Expiration': expiry.astimezone(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}
        apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'aws-ephemeral', 'namespace': NS},
               'stringData': {'creds.json': json.dumps(payload)}})

    def refresh_loop():
        while not stop.wait(15):
            try:
                refresh()
            except Exception as exc:
                log('credential_refresh_error', error=type(exc).__name__)

    def poll(pattern=None):
        params = {'logGroupName': group, 'limit': 10000}
        if pattern:
            params['filterPattern'] = pattern
            params['logStreamNamePrefix'] = 'reused-logs-'
        result, tokens = {}, set()
        while True:
            response = aws.filter_log_events(**params)
            result.update({e['eventId']: e for e in response.get('events', [])})
            token = response.get('nextToken')
            if not token or token in tokens:
                return result
            tokens.add(token)
            params['nextToken'] = token
            time.sleep(.2)

    try:
        identity = session.client('sts').get_caller_identity()
        if identity['Account'] != args.account:
            raise RuntimeError('AWS account does not match explicit --account')
        if k('get', 'namespace', NS, '--ignore-not-found').strip():
            raise RuntimeError('Namespace already exists; refusing ownership')
        k('create', 'namespace', NS)
        owned_ns = True
        aws.create_log_group(logGroupName=group, tags={'Experiment': run, 'ManagedBy': 'kubecon-cfp'})
        owned_group = True
        save('ownership.json', {'group': group, 'region': args.region, 'run': run, 'cluster': args.cluster})
        aws.put_retention_policy(logGroupName=group, retentionInDays=1)
        save('preflight.json', {'region': args.region, 'identity': identity, 'replicas': COUNT,
                               'generations': GENERATIONS, 'records_per_pod': RECORDS})
        refresh()
        refresher = threading.Thread(target=refresh_loop, daemon=True)
        refresher.start()
        conf = configuration(args.region, group)
        (out / 'fluent-bit.conf').write_text(conf)
        apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'fluent-config', 'namespace': NS},
               'data': {'fluent-bit.conf': conf, 'partition.lua': LUA, 'serve.py': SERVER, 'app.py': APP}})
        apply({'apiVersion': 'v1', 'kind': 'ServiceAccount', 'metadata': {'name': 'fluent', 'namespace': NS}})
        apply({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'Role',
               'metadata': {'name': 'metadata-reader', 'namespace': NS},
               'rules': [{'apiGroups': [''], 'resources': ['pods', 'namespaces'], 'verbs': ['get', 'list', 'watch']}]})
        apply({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
               'metadata': {'name': 'metadata-reader', 'namespace': NS},
               'subjects': [{'kind': 'ServiceAccount', 'name': 'fluent', 'namespace': NS}],
               'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'metadata-reader'}})
        mount = {'name': 'config', 'mountPath': '/config', 'readOnly': True}
        apply({'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'fluent', 'namespace': NS},
               'spec': {'serviceAccountName': 'fluent', 'containers': [
                   {'name': 'fluent', 'image': 'cr.fluentbit.io/fluent/fluent-bit:5.1.2',
                    'args': ['-c', '/config/fluent-bit.conf'],
                    'env': [{'name': 'AWS_CONTAINER_CREDENTIALS_FULL_URI', 'value': 'http://127.0.0.1:8181/credentials'}],
                    'volumeMounts': [mount, {'name': 'logs', 'mountPath': '/var/log', 'readOnly': True},
                                     {'name': 'buffers', 'mountPath': '/buffers'}],
                    'resources': {'requests': {'cpu': '100m', 'memory': '64Mi'}},
                    'readinessProbe': {'httpGet': {'path': '/', 'port': 2020}}},
                   {'name': 'credentials', 'image': 'python:3.13.2-slim-bookworm',
                    'command': ['python', '/config/serve.py'],
                    'volumeMounts': [mount, {'name': 'credentials', 'mountPath': '/credentials', 'readOnly': True}],
                    'readinessProbe': {'exec': {'command': ['python', '-c',
                        "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8181/credentials').read()"]}}}],
                   'volumes': [{'name': 'config', 'configMap': {'name': 'fluent-config'}},
                               {'name': 'logs', 'hostPath': {'path': '/var/log', 'type': 'Directory'}},
                               {'name': 'buffers', 'emptyDir': {'sizeLimit': '128Mi'}},
                               {'name': 'credentials', 'secret': {'secretName': 'aws-ephemeral'}}]}})
        k('wait', '-n', NS, 'pod/fluent', '--for=condition=Ready', '--timeout=180s', timeout=200)
        for generation in range(GENERATIONS):
            env = [{'name': 'GENERATION', 'value': str(generation)}]
            env += [{'name': key, 'valueFrom': {'fieldRef': {'fieldPath': field}}}
                    for key, field in [('POD_UID', 'metadata.uid'), ('POD_NAME', 'metadata.name')]]
            apply({'apiVersion': 'apps/v1', 'kind': 'Deployment',
                   'metadata': {'name': 'checkout', 'namespace': NS},
                   'spec': {'replicas': COUNT, 'selector': {'matchLabels': {'app': 'checkout'}},
                            'strategy': {'type': 'RollingUpdate', 'rollingUpdate': {'maxSurge': 4, 'maxUnavailable': 4}},
                            'template': {'metadata': {'labels': {'app': 'checkout', 'generation': str(generation)}},
                                         'spec': {'terminationGracePeriodSeconds': 1,
                                                  'containers': [{'name': 'app', 'image': 'python:3.13.2-slim-bookworm',
                                                                  'command': ['python', '-u', '/config/app.py'],
                                                                  'env': env, 'volumeMounts': [mount],
                                                                  'resources': {'requests': {'cpu': '1m', 'memory': '16Mi'},
                                                                                'limits': {'memory': '48Mi'}}}],
                                                  'volumes': [{'name': 'config', 'configMap': {'name': 'fluent-config'}}]}}}})
            k('rollout', 'status', 'deployment/checkout', '-n', NS, '--timeout=180s', timeout=200)
            pods = json.loads(k('get', 'pods', '-n', NS, '-l', f'app=checkout,generation={generation}', '-o', 'json'))
            save(f'generation-{generation}-pods.json', pods)
            if len(pods['items']) != COUNT:
                raise RuntimeError('Unexpected workload Pod count')
            for pod in pods['items']:
                uid, name = pod['metadata']['uid'], pod['metadata']['name']
                for sequence in range(RECORDS):
                    expected[f'{uid}:{sequence}'] = {'uid': uid, 'name': name, 'generation': generation}
            deadline = time.monotonic() + 150
            while True:
                events.update(poll())
                result = audit(events.values(), expected)
                if result['metadata_errors']:
                    raise RuntimeError(result['metadata_errors'][:3])
                if all(d['missing_records'] == 0 for d in result['designs'].values()):
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError(f'Delivery deadline: {result}')
                time.sleep(5)
            log('generation_delivered', generation=generation, audit=result)
        source = next(iter(expected.values()))
        pattern = '{ $.kubernetes.pod_name = "' + source['name'] + '" }'
        retrieved = poll(pattern)
        save('source-retrieval.json', {'filterPattern': pattern, 'events': list(retrieved.values())})
        ids = {json.loads(e['message'])['record_id'] for e in retrieved.values()}
        target = {rid for rid, value in expected.items() if value['uid'] == source['uid']}
        if ids != target:
            raise RuntimeError('Source retrieval did not return the exact expected record set')
        result['source_retrieval'] = {'expected': len(target), 'observed_unique': len(ids), 'passed': True}
        save('audit.json', result)
        log('validation_passed', expected_per_design=len(expected))
    except Exception:
        (out / 'failure.txt').write_text(traceback.format_exc())
        raise
    finally:
        stop.set()
        if refresher:
            refresher.join(timeout=30)
        save('expected-records.json', expected)
        save('cloudwatch-events.json', list(events.values()))
        if owned_ns:
            try:
                (out / 'fluent.log').write_text(k('logs', '-n', NS, 'fluent', '-c', 'fluent'))
                save('pods-final.json', json.loads(k('get', 'pods', '-n', NS, '-o', 'json')))
            except Exception as exc:
                cleanup['diagnostic_error'] = str(exc)
        if owned_group:
            try:
                tags = aws.list_tags_log_group(logGroupName=group)['tags']
                if tags.get('Experiment') != run:
                    raise RuntimeError('Ownership tag mismatch; refusing deletion')
                aws.delete_log_group(logGroupName=group)
                remaining = aws.describe_log_groups(logGroupNamePrefix=group)['logGroups']
                cleanup['cloudwatch_group_absent'] = all(g['logGroupName'] != group for g in remaining)
            except Exception as exc:
                cleanup['cloudwatch_cleanup_error'] = str(exc)
        if owned_ns:
            try:
                k('delete', 'namespace', NS, '--wait=true', '--timeout=90s', timeout=110)
                cleanup['namespace_absent'] = not k('get', 'namespace', NS, '--ignore-not-found').strip()
            except Exception as exc:
                cleanup['namespace_cleanup_error'] = str(exc)
        save('cleanup.json', cleanup)
        log('cleanup', **cleanup)
        if (owned_group and not cleanup.get('cloudwatch_group_absent')) or (owned_ns and not cleanup.get('namespace_absent')):
            raise RuntimeError('Cleanup incomplete: inspect cleanup.json')


if __name__ == '__main__':
    main()
