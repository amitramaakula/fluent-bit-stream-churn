"""Remove only an interrupted identity run's proven-owned group/namespace."""
import argparse
import json
from pathlib import Path
import subprocess
import boto3

def valid_marker(marker, run):
    return (marker.get('run') == run and run.startswith('identity-cloud-')
            and marker.get('group') == '/kubecon/stream-churn/' + run
            and marker.get('cluster') == 'cwlogs-identity'
            and marker.get('region') == 'us-west-2')

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('run')
    p.add_argument('--account', required=True)
    args = p.parse_args()
    if Path(args.run).name != args.run:
        p.error('run must be a directory name, not a path')
    out = Path(__file__).resolve().parents[1] / 'results' / args.run
    marker = json.loads((out / 'ownership.json').read_text())
    if not valid_marker(marker, args.run):
        raise RuntimeError('Ownership marker is outside the expected experiment scope')
    session = boto3.Session(region_name=marker['region'])
    if session.client('sts').get_caller_identity()['Account'] != args.account:
        raise RuntimeError('Unexpected AWS account')
    client = session.client('logs')
    report = {}
    try:
        groups = client.describe_log_groups(logGroupNamePrefix=marker['group'])['logGroups']
        if any(g['logGroupName'] == marker['group'] for g in groups):
            tags = client.list_tags_log_group(logGroupName=marker['group'])['tags']
            if tags.get('Experiment') != args.run or tags.get('ManagedBy') != 'kubecon-cfp':
                raise RuntimeError('Group tags do not establish ownership')
            client.delete_log_group(logGroupName=marker['group'])
        report['group_absent'] = not any(g['logGroupName'] == marker['group'] for g in client.describe_log_groups(logGroupNamePrefix=marker['group'])['logGroups'])
    except Exception as exc:
        report['group_error'] = str(exc)
    try:
        command = ['kubectl', '--context', 'kind-cwlogs-identity']
        raw = subprocess.check_output(command + ['get', 'namespace', 'identity-cloud', '--ignore-not-found', '-o', 'json'], text=True, timeout=30)
        if raw.strip():
            ns = json.loads(raw)
            if marker.get('namespace') != 'identity-cloud' or ns['metadata']['uid'] != marker.get('namespace_uid'):
                raise RuntimeError('Namespace UID does not establish ownership; inspect manually')
            subprocess.run(command + ['delete', 'namespace', 'identity-cloud', '--timeout=90s'], check=True, timeout=110)
        raw = subprocess.check_output(command + ['get', 'namespace', 'identity-cloud', '--ignore-not-found', '-o', 'json'], text=True, timeout=30)
        report['namespace_absent'] = not raw.strip()
    except Exception as exc:
        report['namespace_error'] = str(exc)
    (out / 'recovery-cleanup.json').write_text(json.dumps(report, indent=2))
    print(json.dumps(report))
    if not report.get('group_absent') or not report.get('namespace_absent'):
        raise SystemExit('Cleanup requires inspection; no cluster deletion was attempted')

if __name__ == '__main__':
    main()
