"""Read the original fleet comparison window from CloudWatch; create no resources."""
import argparse
import datetime as dt
import json
from pathlib import Path

START = '2026-10-11T03:00:00+00:00'
END = '2026-10-11T03:50:00+00:00'
REGION = 'us-west-2'
RUN = 'kubecon-fleet-20261011-0201'


def fetch(client):
    series = []
    for api in ['CreateLogStream', 'PutLogEvents']:
        dimensions = [{'Name': k, 'Value': v} for k, v in
                      [('Service', 'Logs'), ('Class', 'None'), ('Type', 'API'), ('Resource', api)]]
        for metric in ['CallCount', 'ThrottleCount']:
            response = client.get_metric_statistics(
                Namespace='AWS/Logs', MetricName=metric, Dimensions=dimensions,
                StartTime=dt.datetime.fromisoformat(START), EndTime=dt.datetime.fromisoformat(END),
                Period=60, Statistics=['Sum'])
            points = [{'Timestamp': p['Timestamp'].astimezone(dt.timezone.utc).isoformat(),
                       'Sum': p['Sum'], 'Unit': p['Unit']} for p in response['Datapoints']]
            series.append({'namespace': 'AWS/Logs', 'metric': metric, 'api': api,
                           'dimensions': dimensions,
                           'datapoints': sorted(points, key=lambda p: p['Timestamp'])})
    return {'run': RUN, 'region': REGION, 'fetched_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
            'start_inclusive': START, 'end_exclusive': END, 'period_seconds': 60, 'statistic': 'Sum',
            'scope': 'Account/region totals, including probes and any concurrent account activity. '
                     'Empty datapoints are retained and are not treated as zero. '
                     'Only AWS/Logs is used; overlapping AWS/Usage counts are not added.',
            'series': series}


if __name__ == '__main__':
    import boto3
    from botocore.config import Config
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    client = boto3.client('cloudwatch', region_name=REGION,
                         config=Config(retries={'mode': 'standard', 'max_attempts': 3}))
    result = fetch(client)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({'fetched_at_utc': result['fetched_at_utc'],
                      'series': [{'api': s['api'], 'metric': s['metric'],
                                  'points': len(s['datapoints']),
                                  'sum_of_returned_points': sum(p['Sum'] for p in s['datapoints'])}
                                 for s in result['series']]}))
