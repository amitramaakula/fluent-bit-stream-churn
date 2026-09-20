#!/usr/bin/env python3
"""Bounded fleet-demand emulation using measured file identities and real AWS.

Use --isolated for two collectors receiving identical records concurrently.
Synthetic collector generations emulate aggregate demand; no actual fleet claim.
The isolated sweep uses 30/60/100 new names/s for 30/60/60s, draining each stage.
"""
import argparse
import concurrent.futures
import datetime as dt
import json
from pathlib import Path
import subprocess
import threading
import time
import traceback
import urllib.request
import urllib.parse
import boto3
from botocore.config import Config
from rollout import SERVER

ROOT = Path(__file__).resolve().parents[1]
NS = 'identity-cloud'
CLUSTER = 'cwlogs-identity'


def endpoint_order(isolated, reverse=False):
    ports = [23201, 23203] if isolated else [23201]
    return list(reversed(ports)) if reverse else ports


def deployment_order(isolated, reverse=False):
    pods = list(enumerate(['fluent-high', 'fluent-reused'] if isolated else ['fluent']))
    return list(reversed(pods)) if reverse else pods


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--account', required=True)
    parser.add_argument('--identity-run', required=True)
    parser.add_argument('--isolated', action='store_true', help='Separate collectors, memory buffers, shorter 30/60/60-second windows')
    parser.add_argument('--reverse-order', action='store_true', help='Reverse collector startup and per-record endpoint order; stages remain 30/60/100 and designs run concurrently')
    args = parser.parse_args()
    if args.reverse_order and not args.isolated:
        parser.error('--reverse-order requires --isolated')
    source = ROOT / 'results' / args.identity_run
    measured = json.loads((source / 'baseline-records.json').read_text())
    files = sorted({r['destinations']['file_only'] for r in measured if r['variant'] == 'three'})
    assert len(files) == 12
    run = 'identity-cloud-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out = ROOT / 'results' / run
    out.mkdir(parents=True)
    group = '/kubecon/stream-churn/' + run
    region = 'us-west-2'
    session = boto3.Session(region_name=region)
    client = session.client('logs', config=Config(retries={'mode': 'standard', 'max_attempts': 4}))
    events, expected = {}, {}
    stop = threading.Event()
    lock = threading.Lock()
    refresh_thread = monitor_thread = forward = None
    forwards, forward_logs = [], []
    log_processes, log_files, log_paths = [], [], []
    pod_names = ['fluent-high', 'fluent-reused'] if args.isolated else ['fluent']
    owned_ns = owned_group = False
    phase = 'setup'
    stages = []

    def save(name, value):
        (out / name).write_text(json.dumps(value, indent=2, default=str))

    def log(event, **data):
        row = {'time': time.time(), 'phase': phase, 'event': event, **data}
        with (out / 'timeline.jsonl').open('a') as f:
            f.write(json.dumps(row) + '\n')
        print(json.dumps(row), flush=True)

    def k(*commands, data=None, timeout=180):
        r = subprocess.run(['kubectl', '--context', 'kind-' + CLUSTER, *commands], input=data,
                           text=True, capture_output=True, timeout=timeout)
        if r.returncode:
            raise RuntimeError(f'kubectl {commands[:2]}: {r.stderr}')
        return r.stdout

    def apply(resource):
        return k('apply', '-f', '-', data=json.dumps(resource))

    def refresh():
        provider = session.get_credentials()
        c = provider.get_frozen_credentials()
        expiry = getattr(provider, '_expiry_time', None)
        if not c.token or expiry is None:
            raise RuntimeError('Temporary credentials required')
        body = {'AccessKeyId': c.access_key, 'SecretAccessKey': c.secret_key, 'Token': c.token,
                'Expiration': expiry.astimezone(dt.timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}
        apply({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'name': 'credentials', 'namespace': NS},
               'stringData': {'creds.json': json.dumps(body)}})

    def refresh_loop():
        while not stop.wait(15):
            try:
                refresh()
            except Exception as exc:
                log('credential_refresh_error', error=type(exc).__name__)

    def poll():
        params = {'logGroupName': group, 'limit': 10000}
        tokens = set()
        while True:
            response = client.filter_log_events(**params)
            with lock:
                events.update({e['eventId']: e for e in response.get('events', [])})
            token = response.get('nextToken')
            if not token or token in tokens:
                return
            params['nextToken'] = token
            tokens.add(token)
            time.sleep(.15)

    def monitor():
        last_poll = 0
        while not stop.is_set():
            row = {'time': time.time(), 'phase': phase}
            try:
                row['metrics'] = ''
                for port in ([23202, 23204] if args.isolated else [23202]):
                    with urllib.request.urlopen(f'http://127.0.0.1:{port}/api/v1/metrics/prometheus', timeout=5) as r:
                        row['metrics'] += r.read().decode() + '\n'
            except Exception as exc:
                row['metrics_error'] = str(exc)
            if time.time() - last_poll >= 12:
                last_poll = time.time()
                try:
                    poll()
                except Exception as exc:
                    row['reader_error'] = str(exc)
            with (out / 'monitor.jsonl').open('a') as f:
                f.write(json.dumps(row) + '\n')
            stop.wait(3)

    def send(records):
        # Real tailing uses a source-file tag, and separate collector processes
        # do not merge generations into one live input chunk. Preserve both
        # boundaries in emulation instead of mixing destinations in one tag.
        for record in records:
            tag = urllib.parse.quote(record['high_stream'], safe='')
            for port in endpoint_order(args.isolated, args.reverse_order):
                request = urllib.request.Request(f'http://127.0.0.1:{port}/' + tag,
                    data=json.dumps(record).encode(), headers={'Content-Type': 'application/json'})
                with urllib.request.urlopen(request, timeout=15) as response:
                    assert response.status in (200, 201)

    def observed(label):
        found = {'high': set(), 'reused': set()}
        with lock:
            snapshot = list(events.values())
        for e in snapshot:
            r = json.loads(e['message'])
            if r['stage'] == label:
                design = 'reused' if e['logStreamName'].startswith('reused-') else 'high'
                found[design].add(r['record_id'])
        return {d: len(v) for d, v in found.items()}

    try:
        identity = session.client('sts').get_caller_identity()
        assert identity['Account'] == args.account
        if k('get', 'namespace', NS, '--ignore-not-found').strip():
            raise RuntimeError('Namespace exists; refusing ownership')
        quotas = []
        for page in session.client('service-quotas').get_paginator('list_service_quotas').paginate(ServiceCode='logs'):
            quotas += [q for q in page['Quotas'] if 'CreateLogStream' in q['QuotaName']]
        assert len(quotas) == 1 and quotas[0]['Value'] == 50
        save('preflight.json', {'identity': identity, 'quotas': quotas, 'source_run': args.identity_run,
                               'files': files, 'real_collectors': len(pod_names), 'region': region,
                               'isolated_collectors_memory_buffers': args.isolated,
                               'reverse_order': args.reverse_order,
                               'collector_startup_order': [name for _, name in deployment_order(args.isolated, args.reverse_order)],
                               'per_record_endpoint_order': endpoint_order(args.isolated, args.reverse_order),
                               'stage_order': [30, 60, 100],
                               'input_chunk_boundary': 'collector generation plus source file; one destination per tag',
                               'emulation': 'synthetic collector generations; actual application files held fixed'})
        k('create', 'namespace', NS)
        owned_ns = True
        client.create_log_group(logGroupName=group, tags={'Experiment': run, 'ManagedBy': 'kubecon-cfp'})
        owned_group = True
        client.put_retention_policy(logGroupName=group, retentionInDays=1)
        save('ownership.json', {'group': group, 'run': run, 'region': region, 'cluster': CLUSTER,
                               'namespace': NS, 'namespace_uid': json.loads(k('get', 'namespace', NS, '-o', 'json'))['metadata']['uid']})
        (out / 'runner-source.py').write_text(Path(__file__).read_text())
        refresh()
        refresh_thread = threading.Thread(target=refresh_loop, daemon=True)
        refresh_thread.start()
        conf = '''[SERVICE]
    Flush 0.5
    Log_Level info
    HTTP_Server On
    HTTP_Listen 0.0.0.0
    HTTP_Port 2020
    storage.path /buffers
    scheduler.base 2
    scheduler.cap 15
[INPUT]
    Name http
    Listen 0.0.0.0
    Port 9880
    storage.type filesystem
'''
        for design, template in [('high', '$high_stream'), ('reused', '$reused_stream')]:
            conf += f'''[OUTPUT]
    Name cloudwatch_logs
    Alias {design}
    Match *
    region {region}
    log_group_name {group}
    log_stream_name ERROR-{design}
    log_stream_template {template}
    auto_create_group false
    auto_retry_requests true
    workers 8
    Retry_Limit False
    log_suppress_interval 0
    storage.total_limit_size 128M
'''
        (out / 'fluent-bit.conf').write_text(conf)
        apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'config', 'namespace': NS},
               'data': {'fluent-bit.conf': conf, 'serve.py': SERVER}})
        mount = {'name': 'config', 'mountPath': '/config', 'readOnly': True}
        pod_template = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': 'fluent', 'namespace': NS},
               'spec': {'containers': [
                   {'name': 'fluent', 'image': 'cr.fluentbit.io/fluent/fluent-bit:5.1.2',
                    'args': ['-c', '/config/fluent-bit.conf'],
                    'env': [{'name': 'AWS_CONTAINER_CREDENTIALS_FULL_URI', 'value': 'http://127.0.0.1:8181/credentials'},
                            {'name': 'AWS_EC2_METADATA_DISABLED', 'value': 'true'}],
                    'volumeMounts': [mount, {'name': 'buffers', 'mountPath': '/buffers'}],
                    'readinessProbe': {'httpGet': {'path': '/', 'port': 2020}}},
                   {'name': 'credentials', 'image': 'python:3.13.2-slim-bookworm',
                    'command': ['python', '/config/serve.py'],
                    'volumeMounts': [mount, {'name': 'credentials', 'mountPath': '/credentials', 'readOnly': True}]}],
                    'volumes': [{'name': 'config', 'configMap': {'name': 'config'}},
                                {'name': 'buffers', 'emptyDir': {'sizeLimit': '256Mi'}},
                                {'name': 'credentials', 'secret': {'secretName': 'credentials'}}]}}
        for index, pod_name in deployment_order(args.isolated, args.reverse_order):
            pod = json.loads(json.dumps(pod_template))
            pod['metadata']['name'] = pod_name
            if args.isolated:
                base, high, reused = conf.split('[OUTPUT]')
                single = base + '[OUTPUT]' + [high, reused][index]
                single = single.replace('    storage.path /buffers\n', '').replace('    storage.type filesystem', '    storage.type memory\n    Mem_Buf_Limit 128M').replace('    storage.total_limit_size 128M\n', '')
                (out / (pod_name + '.conf')).write_text(single)
                apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': pod_name, 'namespace': NS},
                       'data': {'fluent-bit.conf': single, 'serve.py': SERVER}})
                pod['spec']['volumes'][0]['configMap']['name'] = pod_name
            apply(pod)
            k('wait', '-n', NS, 'pod/' + pod_name, '--for=condition=Ready', '--timeout=180s', timeout=200)
            log_path = out / (pod_name + '-live.log')
            log_paths.append(log_path)
            log_file = log_path.open('w')
            log_files.append(log_file)
            log_processes.append(subprocess.Popen(['kubectl', '--context', 'kind-' + CLUSTER,
                '-n', NS, 'logs', '-f', pod_name, '-c', 'fluent'], stdout=log_file, stderr=log_file))
            forward_log = (out / (pod_name + '-forward.log')).open('w')
            forward_logs.append(forward_log)
            forwards.append(subprocess.Popen(['kubectl', '--context', 'kind-' + CLUSTER, '-n', NS, 'port-forward',
                                'pod/' + pod_name, f'{23201 + index * 2}:9880', f'{23202 + index * 2}:2020'], stdout=forward_log, stderr=forward_log))
        time.sleep(2)
        monitor_thread = threading.Thread(target=monitor, daemon=True)
        monitor_thread.start()
        for rate, duration in ([(30, 30), (60, 60), (100, 60)] if args.isolated else [(30, 60), (60, 90), (100, 90)]):
            label = f'rate-{rate}'
            phase = label + '-sending'
            stage = {'stage': label, 'target_new_streams_per_second': rate, 'duration_seconds': duration,
                     'start': time.time(), 'expected_per_design': rate * duration}
            stages.append(stage)
            log('stage_start', **stage)
            start = time.monotonic()
            futures = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                for tick in range(duration * 5):
                    delay = start + tick / 5 - time.monotonic()
                    if delay > 0:
                        time.sleep(delay)
                    batch = []
                    for offset in range(rate // 5):
                        i = tick * (rate // 5) + offset
                        rid = f'{label}-{i:06d}'
                        record = {'record_id': rid, 'stage': label, 'created_ms': int(time.time() * 1000),
                                  'source_file': files[i % 12], 'collector_generation': i // 12,
                                  'high_stream': f'high-{label}-collector-{i // 12:05d}.' + files[i % 12],
                                  'reused_stream': f'reused-{i % 32:02d}'}
                        expected[rid] = record
                        batch.append(record)
                    futures.append(pool.submit(send, batch))
                for future in futures:
                    future.result()
            stage['send_end'] = time.time()
            stage['actual_send_seconds'] = time.monotonic() - start
            phase = label + '-draining'
            log('send_complete', observed=observed(label), seconds=stage['actual_send_seconds'])
            deadline = time.monotonic() + 300
            last_report = 0
            while True:
                seen = observed(label)
                if all(n == rate * duration for n in seen.values()):
                    break
                if time.monotonic() > deadline:
                    raise RuntimeError(f'Drain deadline: {label} {seen}')
                if time.monotonic() - last_report > 25:
                    last_report = time.monotonic()
                    log('drain_progress', observed=seen, expected=rate * duration)
                time.sleep(3)
            stage['drain_observed_complete'] = time.time()
            log('stage_complete', observed=seen)
            save('stages.json', stages)
            (out / (label + '-fluent.log')).write_text('\n'.join(k('logs', '-n', NS, name, '-c', 'fluent') for name in pod_names))
        log('validation_complete', expected_per_design=len(expected))
    except Exception:
        (out / 'failure.txt').write_text(traceback.format_exc())
        raise
    finally:
        stop.set()
        if refresh_thread:
            refresh_thread.join(timeout=30)
        if monitor_thread:
            monitor_thread.join(timeout=45)
        save('stages.json', stages)
        save('expected-records.json', expected)
        with lock:
            save('cloudwatch-events.json', list(events.values()))
        cleanup = {}
        if owned_ns:
            try:
                (out / 'final-fluent.log').write_text('\n'.join(k('logs', '-n', NS, name, '-c', 'fluent') for name in pod_names))
                save('pods-final.json', json.loads(k('get', 'pods', '-n', NS, '-o', 'json')))
            except Exception as exc:
                cleanup['diagnostic_error'] = str(exc)
        for forward in forwards:
            try:
                forward.terminate()
                forward.wait(timeout=15)
            except Exception as exc:
                cleanup.setdefault('local_process_errors', []).append(type(exc).__name__)
        for forward_log in forward_logs:
            forward_log.close()
        for process in log_processes:
            try:
                process.terminate()
                process.wait(timeout=15)
            except Exception as exc:
                cleanup.setdefault('local_process_errors', []).append(type(exc).__name__)
        for file in log_files:
            file.close()
        if log_paths:
            (out / 'final-fluent.log').write_text('\n'.join(path.read_text() for path in log_paths))
        if owned_group:
            try:
                assert client.list_tags_log_group(logGroupName=group)['tags']['Experiment'] == run
                client.delete_log_group(logGroupName=group)
                cleanup['cloudwatch_group_absent'] = not any(g['logGroupName'] == group for g in
                    client.describe_log_groups(logGroupNamePrefix=group)['logGroups'])
            except Exception as exc:
                cleanup['cloudwatch_cleanup_error'] = str(exc)
        if owned_ns:
            try:
                k('delete', 'namespace', NS, '--timeout=90s', timeout=110)
                cleanup['namespace_absent'] = not k('get', 'namespace', NS, '--ignore-not-found').strip()
            except Exception as exc:
                cleanup['namespace_cleanup_error'] = str(exc)
        save('cleanup.json', cleanup)
        log('cleanup', **cleanup)
        if (owned_group and not cleanup.get('cloudwatch_group_absent')) or (owned_ns and not cleanup.get('namespace_absent')):
            raise RuntimeError('Cleanup incomplete; inspect cleanup.json')


if __name__ == '__main__':
    main()
