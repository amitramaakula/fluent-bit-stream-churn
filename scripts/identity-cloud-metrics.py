"""Read-only usage metrics and independent cleanup verification after cloud runs."""
import datetime as dt
import json
import math
from pathlib import Path
import subprocess
import sys
import boto3

root = Path(__file__).resolve().parents[1] / 'results' / sys.argv[1]
ownership = json.loads((root / 'ownership.json').read_text())
rows = [json.loads(line) for line in (root / 'timeline.jsonl').read_text().splitlines()]
start_seconds = min(r['time'] for r in rows)
end_seconds = max(r['time'] for r in rows)
start = dt.datetime.fromtimestamp(math.floor(start_seconds / 60) * 60, dt.timezone.utc)
end = dt.datetime.fromtimestamp(math.ceil(end_seconds / 60) * 60, dt.timezone.utc)
session = boto3.Session(region_name=ownership['region'])
client = session.client('cloudwatch')
series = []
for api in ['CreateLogStream', 'PutLogEvents']:
    dimensions = [{'Name': k, 'Value': v} for k, v in {'Service': 'Logs', 'Class': 'None', 'Type': 'API', 'Resource': api}.items()]
    for namespace, metric in [('AWS/Usage', 'CallCount'), ('AWS/Logs', 'ThrottleCount')]:
        response = client.get_metric_statistics(Namespace=namespace, MetricName=metric, Dimensions=dimensions,
                                               StartTime=start, EndTime=end, Period=60, Statistics=['Sum'])
        points = [{'time': p['Timestamp'].isoformat(), 'sum': p['Sum']}
                  for p in sorted(response['Datapoints'], key=lambda p: p['Timestamp'])]
        series.append({'api': api, 'metric': metric, 'namespace': namespace, 'points': points,
                       'observed_sum': sum(p['sum'] for p in points)})
data = {'start': start.isoformat(), 'end': end.isoformat(), 'retrieved_at': dt.datetime.now(dt.timezone.utc).isoformat(),
        'scope': 'Account/region, both designs together; minute bins can cross stage boundaries. Missing datapoints are not proof of zero.',
        'series': series}
(root / 'cloudwatch-api-usage.json').write_text(json.dumps(data, indent=2))
groups = session.client('logs').describe_log_groups(logGroupNamePrefix=ownership['group'])['logGroups']
assert not any(g['logGroupName'] == ownership['group'] for g in groups)
clusters = subprocess.check_output(['kind', 'get', 'clusters'], text=True).splitlines()
verification = {'checked_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'cloudwatch_group_absent': True,
                'owned_kind_cluster_absent': ownership['cluster'] not in clusters}
all_groups = {}
for folder in root.parent.glob('identity-cloud-*'):
    marker = folder / 'ownership.json'
    if not marker.exists():
        continue
    owned = json.loads(marker.read_text())
    remaining = session.client('logs').describe_log_groups(logGroupNamePrefix=owned['group'])['logGroups']
    all_groups[folder.name] = not any(g['logGroupName'] == owned['group'] for g in remaining)
assert all(all_groups.values())
verification['all_identity_run_groups_absent'] = all_groups
(root / 'independent-cleanup.json').write_text(json.dumps(verification, indent=2))
print(json.dumps({'series_sums': [{'api': s['api'], 'metric': s['metric'], 'sum': s['observed_sum']} for s in series],
                  'cleanup': verification}))
