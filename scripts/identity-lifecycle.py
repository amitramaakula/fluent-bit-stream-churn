#!/usr/bin/env python3
"""Measure real lifecycle identity changes locally; no AWS resources.

Requires the disposable kind-cwlogs-identity cluster. Writes evidence per event.
Both workload variants emit 48 records per phase: four Pods with one container
versus four Pods with three containers. Log rotation is explicitly excluded.
"""
import concurrent.futures
import datetime as dt
import json
from pathlib import Path
import subprocess
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]
NS = 'identity-lab'
CLUSTER = 'cwlogs-identity'
RUN = 'identity-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
OUT = ROOT / 'results' / RUN
OUT.mkdir(parents=True)

APP = '''import json,os,time,uuid
from pathlib import Path
boot=uuid.uuid4().hex
last=None
while True:
 p=Path('/tmp/phase')
 phase=p.read_text() if p.exists() else None
 if phase and phase != last:
  for i in range(int(os.environ['RECORDS'])):
   print(json.dumps({'record_id':boot+':'+phase+':'+str(i),'phase':phase,
    'source_uid':os.environ['POD_UID'],'source_container':os.environ['CONTAINER'],
    'variant':os.environ['VARIANT'],'created_ms':int(time.time()*1000)}),flush=True)
   time.sleep(.05)
  last=phase
 time.sleep(.1)
'''
LUA = '''function names(tag, ts, record)
 local k=record['kubernetes']
 if k == nil or k['pod_name'] == nil then return -1,ts,record end
 local file=string.match(record['source_file'],'([^/]+)$')
 record['destinations']={stable='shared', pod_container=k['pod_name']..'.'..k['container_name'],
  file_only=file, collector_only=os.getenv('POD_NAME'),
  collector_file=os.getenv('POD_NAME')..'.'..file}
 return 1,ts,record
end
'''
CONF = '''[SERVICE]
    Flush 1
    Log_Level info
    Parsers_File /fluent-bit/etc/parsers.conf
    HTTP_Server On
    HTTP_Listen 0.0.0.0
    HTTP_Port 2020
[INPUT]
    Name tail
    Path /var/log/containers/work-*_identity-lab_app*.log
    Path_Key source_file
    Tag kube.*
    multiline.parser cri
    Read_from_Head On
    Refresh_Interval 1
    DB /buffers/tail.db
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
    script /config/names.lua
    call names
[OUTPUT]
    Name stdout
    Match *
    Format json_lines
'''


def k(*args, data=None, check=True, timeout=180):
    r = subprocess.run(['kubectl', '--context', 'kind-' + CLUSTER, *args], input=data,
                       text=True, capture_output=True, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f'kubectl {args[:3]}: {r.stderr}')
    return r.stdout


def save(name, value):
    (OUT / name).write_text(json.dumps(value, indent=2))


def apply(resource):
    return k('apply', '-f', '-', data=json.dumps(resource))


def pods(selector):
    items = json.loads(k('get', 'pods', '-n', NS, '-l', selector, '-o', 'json'))['items']
    return [p for p in items if not p['metadata'].get('deletionTimestamp')
            and p['status'].get('phase') not in ('Succeeded', 'Failed')]


def collector():
    return next(p for p in pods('app=collector') if not p['metadata'].get('deletionTimestamp'))


def records(raw):
    result = []
    for line in raw.splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, list):
                value = value[-1]
            if isinstance(value, dict) and 'record_id' in value:
                result.append(value)
        except (ValueError, TypeError):
            pass
    return result


def workload(variant, generation):
    count = 1 if variant == 'one' else 3
    containers = []
    for i in range(count):
        containers.append({'name': f'app{i}', 'image': 'python:3.13.2-slim-bookworm',
            'command': ['python', '-u', '/config/app.py'],
            'env': [{'name': 'POD_UID', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.uid'}}},
                    {'name': 'CONTAINER', 'value': f'app{i}'}, {'name': 'VARIANT', 'value': variant},
                    {'name': 'RECORDS', 'value': str(12 // count)}],
            'volumeMounts': [{'name': 'config', 'mountPath': '/config', 'readOnly': True}],
            'resources': {'requests': {'cpu': '1m', 'memory': '16Mi'}, 'limits': {'memory': '48Mi'}}})
    return {'apiVersion': 'apps/v1', 'kind': 'Deployment',
        'metadata': {'name': 'work-' + variant, 'namespace': NS},
        'spec': {'replicas': 4, 'selector': {'matchLabels': {'app': 'work-' + variant}},
            'template': {'metadata': {'labels': {'app': 'work-' + variant, 'workload': 'yes'},
                                      'annotations': {'generation': str(generation)}},
                         'spec': {'terminationGracePeriodSeconds': 1, 'containers': containers,
                                  'volumes': [{'name': 'config', 'configMap': {'name': 'identity-config'}}]}}}}


def main():
    owned = False
    phases = []
    try:
        if k('get', 'namespace', NS, '--ignore-not-found').strip():
            raise RuntimeError('Namespace already exists; refusing ownership')
        k('create', 'namespace', NS)
        owned = True
        (OUT / 'runner-source.py').write_text(Path(__file__).read_text())
        (OUT / 'fluent-bit.conf').write_text(CONF)
        (OUT / 'names.lua').write_text(LUA)
        apply({'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': 'identity-config', 'namespace': NS},
               'data': {'fluent-bit.conf': CONF, 'names.lua': LUA, 'app.py': APP}})
        apply({'apiVersion': 'v1', 'kind': 'ServiceAccount', 'metadata': {'name': 'collector', 'namespace': NS}})
        apply({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'Role',
               'metadata': {'name': 'metadata', 'namespace': NS},
               'rules': [{'apiGroups': [''], 'resources': ['pods'], 'verbs': ['get', 'list', 'watch']}]})
        apply({'apiVersion': 'rbac.authorization.k8s.io/v1', 'kind': 'RoleBinding',
               'metadata': {'name': 'metadata', 'namespace': NS},
               'subjects': [{'kind': 'ServiceAccount', 'name': 'collector', 'namespace': NS}],
               'roleRef': {'apiGroup': 'rbac.authorization.k8s.io', 'kind': 'Role', 'name': 'metadata'}})
        apply({'apiVersion': 'apps/v1', 'kind': 'Deployment', 'metadata': {'name': 'collector', 'namespace': NS},
               'spec': {'replicas': 1, 'strategy': {'type': 'Recreate'},
                        'selector': {'matchLabels': {'app': 'collector'}},
                        'template': {'metadata': {'labels': {'app': 'collector'}}, 'spec': {
                            'serviceAccountName': 'collector', 'terminationGracePeriodSeconds': 5,
                            'containers': [{'name': 'fluent', 'image': 'cr.fluentbit.io/fluent/fluent-bit:5.1.2',
                                'args': ['-c', '/config/fluent-bit.conf'],
                                'env': [{'name': 'POD_NAME', 'valueFrom': {'fieldRef': {'fieldPath': 'metadata.name'}}}],
                                'volumeMounts': [{'name': 'config', 'mountPath': '/config', 'readOnly': True},
                                                 {'name': 'logs', 'mountPath': '/var/log', 'readOnly': True},
                                                 {'name': 'buffers', 'mountPath': '/buffers'}],
                                'readinessProbe': {'httpGet': {'path': '/', 'port': 2020}}}],
                            'volumes': [{'name': 'config', 'configMap': {'name': 'identity-config'}},
                                        {'name': 'logs', 'hostPath': {'path': '/var/log'}},
                                        {'name': 'buffers', 'hostPath': {'path': '/var/lib/identity-lab', 'type': 'DirectoryOrCreate'}}]}}}})
        for variant in ['one', 'three']:
            apply(workload(variant, 0))
        for name in ['collector', 'work-one', 'work-three']:
            k('rollout', 'status', 'deployment/' + name, '-n', NS, '--timeout=180s', timeout=200)
        for phase in ['baseline', 'container-restart', 'application-replacement', 'collector-container-restart', 'collector-replacement']:
            start = time.monotonic()
            if phase == 'container-restart':
                targets = [c['containerID'].split('://')[1] for p in pods('workload=yes') for c in p['status']['containerStatuses']]
                with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                    futures = [pool.submit(subprocess.run, ['docker', 'exec', CLUSTER + '-control-plane',
                                           'crictl', 'stop', '--timeout=1', cid], check=True, capture_output=True) for cid in targets]
                    for future in futures:
                        future.result()
                deadline = time.monotonic() + 120
                while True:
                    status = [c for p in pods('workload=yes') for c in p['status'].get('containerStatuses', [])]
                    if len(status) == 16 and all(c['restartCount'] >= 1 and c.get('ready') for c in status):
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError('Container restarts did not become ready')
                    time.sleep(2)
            elif phase == 'application-replacement':
                for variant in ['one', 'three']:
                    apply(workload(variant, 1))
                    k('rollout', 'status', 'deployment/work-' + variant, '-n', NS, '--timeout=180s', timeout=200)
            elif phase == 'collector-container-restart':
                before = collector()
                cid = before['status']['containerStatuses'][0]['containerID'].split('://')[1]
                subprocess.run(['docker', 'exec', CLUSTER + '-control-plane', 'crictl', 'stop', cid], check=True, capture_output=True)
                deadline = time.monotonic() + 120
                while True:
                    status = collector()['status'].get('containerStatuses', [])
                    if status and status[0]['restartCount'] >= 1 and status[0].get('ready'):
                        break
                    if time.monotonic() > deadline:
                        raise RuntimeError('Collector container did not recover')
                    time.sleep(2)
            elif phase == 'collector-replacement':
                k('rollout', 'restart', 'deployment/collector', '-n', NS)
                k('rollout', 'status', 'deployment/collector', '-n', NS, '--timeout=180s', timeout=200)
            snapshot = pods('workload=yes')
            save(phase + '-pods.json', snapshot)
            cpod = collector()
            save(phase + '-collector.json', cpod)
            targets = [(p['metadata']['name'], c['name']) for p in snapshot for c in p['spec']['containers']]
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
                futures = [pool.submit(k, 'exec', '-n', NS, p, '-c', c, '--', 'python', '-c',
                            "from pathlib import Path; Path('/tmp/phase').write_text(" + repr(phase) + ")") for p, c in targets]
                for future in futures:
                    future.result()
            deadline = time.monotonic() + 60
            while True:
                raw = k('logs', '-n', NS, cpod['metadata']['name'])
                data = [r for r in records(raw) if r['phase'] == phase]
                if len({r['record_id'] for r in data}) == 96:
                    break
                if time.monotonic() > deadline:
                    (OUT / (phase + '-fluent.log')).write_text(raw)
                    raise RuntimeError(f'{phase}: received {len(data)} records; expected 96')
                time.sleep(2)
            (OUT / (phase + '-fluent.log')).write_text(raw)
            save(phase + '-records.json', data)
            lookup = {(p['metadata']['uid'], c['name']): c['containerID'].split('://')[1]
                      for p in snapshot for c in p['status']['containerStatuses']}
            for record in data:
                assert record['kubernetes']['pod_id'] == record['source_uid']
                assert lookup[(record['source_uid'], record['source_container'])] in record['source_file']
            result = {'phase': phase, 'event_and_delivery_seconds': time.monotonic() - start, 'variants': {}}
            for variant in ['one', 'three']:
                subset = [r for r in data if r['variant'] == variant]
                assert len(subset) == 48
                result['variants'][variant] = {'records': len(subset), 'destinations': {
                    key: sorted({r['destinations'][key] for r in subset}) for key in subset[0]['destinations']}}
            phases.append(result)
            save('phases.json', phases)
            print(json.dumps({'phase': phase, 'seconds': round(result['event_and_delivery_seconds'], 2),
                              'records': len(data), 'metadata_verified': True}), flush=True)
        summary = []
        for index, phase in enumerate(phases):
            for variant, info in phase['variants'].items():
                for key, values in info['destinations'].items():
                    previous = set(phases[index - 1]['variants'][variant]['destinations'][key]) if index else set()
                    summary.append({'phase': phase['phase'], 'variant': variant, 'scheme': key,
                                    'current_destinations': len(values), 'new_since_previous_phase': len(set(values) - previous)})
        save('identity-matrix.json', summary)
        print(json.dumps({'passed': True, 'run': RUN, 'output': str(OUT)}), flush=True)
    except Exception:
        (OUT / 'failure.txt').write_text(traceback.format_exc())
        raise
    finally:
        if owned:
            k('delete', 'namespace', NS, '--timeout=90s', timeout=110)
            save('cleanup.json', {'namespace_absent': not k('get', 'namespace', NS, '--ignore-not-found').strip(),
                                  'aws_resources_created': False, 'cluster_lifecycle': 'managed separately'})


if __name__ == '__main__':
    main()
