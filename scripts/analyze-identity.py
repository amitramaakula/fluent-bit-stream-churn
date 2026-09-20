"""Independent lifecycle assertions, CloudWatch record audit and evidence plots."""
import argparse
from collections import Counter, defaultdict
import datetime as dt
import json
import math
from pathlib import Path
import re
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parents[1]


def load(path):
    return json.loads(path.read_text())


def percentile(values, fraction):
    return sorted(values)[max(0, math.ceil(len(values) * fraction) - 1)] if values else None


def lifecycle(run):
    phases = ['baseline', 'container-restart', 'application-replacement', 'collector-container-restart', 'collector-replacement']
    snapshots = {p: load(run / (p + '-pods.json')) for p in phases}
    collectors = {p: load(run / (p + '-collector.json')) for p in phases}
    uidsets = {p: {pod['metadata']['uid'] for pod in snapshots[p]} for p in phases}
    cids = {p: {c['containerID'] for pod in snapshots[p] for c in pod['status']['containerStatuses']} for p in phases}
    assert uidsets['baseline'] == uidsets['container-restart']
    assert cids['baseline'].isdisjoint(cids['container-restart'])
    assert uidsets['container-restart'].isdisjoint(uidsets['application-replacement'])
    assert cids['application-replacement'] == cids['collector-container-restart'] == cids['collector-replacement']
    assert collectors['application-replacement']['metadata']['uid'] == collectors['collector-container-restart']['metadata']['uid']
    assert collectors['collector-container-restart']['metadata']['uid'] != collectors['collector-replacement']['metadata']['uid']
    assert collectors['application-replacement']['status']['containerStatuses'][0]['containerID'] != collectors['collector-container-restart']['status']['containerStatuses'][0]['containerID']
    all_ids = set()
    for p in phases:
        records = load(run / (p + '-records.json'))
        assert len(records) == len({r['record_id'] for r in records}) == 96
        assert Counter(r['variant'] for r in records) == {'one': 48, 'three': 48}
        assert not all_ids.intersection(r['record_id'] for r in records)
        all_ids.update(r['record_id'] for r in records)
    matrix = load(run / 'identity-matrix.json')
    schemes = ['stable', 'pod_container', 'file_only', 'collector_only', 'collector_file']
    expected_three = {
        'container-restart': [0, 0, 12, 0, 12],
        'application-replacement': [0, 12, 12, 0, 12],
        'collector-container-restart': [0, 0, 0, 0, 0],
        'collector-replacement': [0, 0, 0, 1, 12]}
    for phase, expected in expected_three.items():
        actual = [next(row['new_since_previous_phase'] for row in matrix
                       if row['phase'] == phase and row['variant'] == 'three' and row['scheme'] == scheme) for scheme in schemes]
        assert actual == expected, (phase, actual)
    fig, ax = plt.subplots(figsize=(9.2, 4.2), layout='constrained')
    cells = list(expected_three.values())
    ax.imshow(cells, cmap='Blues', vmin=0, vmax=12, aspect='auto')
    ax.set_xticks(range(5), ['Stable', 'Pod + container\nname', 'Full source\nfilename', 'Collector\nPod name', 'Collector +\nfilename'])
    ax.set_yticks(range(4), ['Application container restart', 'Application Pod replacement', 'Collector container restart', 'Collector Pod replacement'])
    for row in range(4):
        for col in range(5):
            value = cells[row][col]
            ax.text(col, row, str(value), ha='center', va='center', color='white' if value > 6 else '#153250', fontsize=14)
    ax.set_title('New destination names depend on the event and naming configuration\nFour application Pods × three containers; 48 records per phase', loc='left', fontsize=12)
    fig.savefig(run / 'identity-matrix.png', dpi=180)
    fig.savefig(run / 'identity-matrix.svg')
    verification = {'passed': True, 'unique_records': len(all_ids), 'phases': len(phases),
                    'application_uid_and_container_id_transitions_verified': True,
                    'collector_uid_and_container_id_transitions_verified': True,
                    'matrix_is_computed_destination_names_not_aws_api_counts': True}
    (run / 'independent-audit.json').write_text(json.dumps(verification, indent=2))
    print(json.dumps({'lifecycle': verification}))


def cloud(run):
    expected = load(run / 'expected-records.json')
    events = load(run / 'cloudwatch-events.json')
    stages = load(run / 'stages.json')
    assert expected and stages, 'Empty or incomplete run is not a completed comparison'
    assert len({s['stage'] for s in stages}) == len(stages), 'Duplicate stage names'
    assert {r['stage'] for r in expected.values()} == {s['stage'] for s in stages}, 'Stage inventory mismatch'
    for stage in stages:
        assert sum(r['stage'] == stage['stage'] for r in expected.values()) == stage['expected_per_design'], 'Incomplete expected-record inventory'
    assert len({e['eventId'] for e in events}) == len(events)
    deliveries = defaultdict(list)
    by_stage = defaultdict(list)
    for e in events:
        r = json.loads(e['message'])
        rid = r['record_id']
        assert rid in expected
        assert r == expected[rid], f'Payload mismatch for {rid}'
        design = 'reused' if e['logStreamName'].startswith('reused-') else 'high'
        assert e['logStreamName'] == expected[rid][design + '_stream']
        deliveries[(design, rid)].append(e)
        by_stage[(r['stage'], design)].append(e)
    parsed_logs = []
    raw = (run / 'final-fluent.log').read_text()
    # Older runs captured periodic snapshots, which can overlap or be truncated
    # by kubelet diagnostic-log retention. Preserve maximum multiplicity per
    # timestamped line across snapshots, and report incomplete stage coverage.
    live_capture = bool(list(run.glob('*-live.log')))
    log_union = Counter(raw.splitlines())
    if not live_capture:
        for snapshot in run.glob('rate-*-fluent.log'):
            log_union |= Counter(snapshot.read_text().splitlines())
    for line in log_union.elements():
        match = re.match(r'\[(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d\.\d+)\].*?\[output:cloudwatch_logs:(high|reused)\] (.*)', line)
        if match:
            timestamp = dt.datetime.strptime(match[1], '%Y/%m/%d %H:%M:%S.%f').replace(tzinfo=dt.timezone.utc).timestamp()
            parsed_logs.append((timestamp, match[2], match[3]))
    summary = []
    for stage in stages:
        label = stage['stage']
        ids = {rid for rid, r in expected.items() if r['stage'] == label}
        for design in ['high', 'reused']:
            missing = [rid for rid in ids if not deliveries.get((design, rid))]
            lags = [(min(e['ingestionTime'] for e in deliveries[(design, rid)]) - expected[rid]['created_ms']) / 1000
                    for rid in ids if deliveries.get((design, rid))]
            logs = [(t, msg) for t, d, msg in parsed_logs if d == design and stage['start'] <= t < stage.get('drain_observed_complete', float('inf'))]
            source_log = raw if live_capture else (run / (label + '-fluent.log')).read_text()
            timestamps = re.findall(r'^\[(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d\.\d+)\]', source_log, re.M)
            first_archived = min(dt.datetime.strptime(t, '%Y/%m/%d %H:%M:%S.%f').replace(tzinfo=dt.timezone.utc).timestamp() for t in timestamps)
            complete_log_capture = live_capture or first_archived <= stage['start']
            bins = [0] * math.ceil(stage['duration_seconds'] / 10)
            for t, msg in logs:
                if 'ThrottlingException' in msg and t < stage['send_end']:
                    index = int((t - stage['start']) // 10)
                    if index < len(bins):
                        bins[index] += 1
            if not complete_log_capture:
                bins = [None if stage['start'] + (i + 1) * 10 <= first_archived else n for i, n in enumerate(bins)]
            row = {'stage': label, 'design': design, 'unique_records': len(lags), 'missing': len(missing),
                   'delivered_records': len(by_stage[(label, design)]),
                   'duplicates': len(by_stage[(label, design)]) - len(lags),
                   'streams': len({e['logStreamName'] for e in by_stage[(label, design)]}),
                   'p95_first_ingestion_seconds': percentile(lags, .95), 'max_first_ingestion_seconds': max(lags) if lags else None,
                   'creation_attempt_log_lines': sum('Creating log stream ' in msg for _, msg in logs),
                   'log_capture_complete': complete_log_capture,
                   'creation_success_log_lines': sum('Created log stream ' in msg for _, msg in logs),
                   'already_exists_log_lines': sum(' already exists' in msg for _, msg in logs),
                   'creation_throttling_responses': sum('CreateLogStream' in msg and 'ThrottlingException' in msg for _, msg in logs),
                   'put_throttling_responses': sum('PutLogEvents' in msg and 'ThrottlingException' in msg for _, msg in logs),
                   'send_window_throttles_per_10s': bins,
                   'last_first_ingestion_after_send_end_seconds': max(0, max((min(e['ingestionTime'] for e in deliveries[(design, rid)]) / 1000
                       for rid in ids if deliveries.get((design, rid))), default=stage['send_end']) - stage['send_end'])}
            summary.append(row)
    monitor = [json.loads(line) for line in (run / 'monitor.jsonl').read_text().splitlines()]
    final_metrics = {}
    for sample in monitor:
        for line in sample.get('metrics', '').splitlines():
            match = re.match(r'(fluentbit_output_(?:errors|retries|proc_records|retries_failed|dropped_records)_total)\{name="(high|reused)"\} ([\d.eE+-]+)', line)
            if match:
                final_metrics[match[1] + ':' + match[2]] = float(match[3])
    audit = {'rows': summary, 'expected_per_design': len(expected), 'cloudwatch_event_ids': len(events),
             'monitor_errors': sum('metrics_error' in r for r in monitor),
             'reader_errors': sum('reader_error' in r for r in monitor),
             'credential_parse_errors': raw.count('Missing AccessKeyId'),
             'storage_mmap_diagnostics': sum(count for line, count in log_union.items() if 'cannot mmap/read chunk' in line),
             'final_observed_metrics': final_metrics,
             'all_unique_records_delivered': all(r['missing'] == 0 for r in summary)}
    (run / 'audit.json').write_text(json.dumps(audit, indent=2))
    fig, axes = plt.subplots(1, 2, figsize=(10, 4.4), layout='constrained')
    for design, color in [('high', '#bb493c'), ('reused', '#167875')]:
        selected = [r for r in summary if r['design'] == design]
        xs = [int(r['stage'].split('-')[1]) for r in selected]
        label = 'Collector + file' if design == 'high' else '32 reused names'
        axes[0].plot(xs, [r['p95_first_ingestion_seconds'] for r in selected], 'o-', color=color, label=label)
        axes[1].plot(xs, [r['creation_throttling_responses'] for r in selected], 'o-', color=color, label=label)
    axes[0].set_ylabel('p95 first-ingestion delay (seconds)')
    axes[1].set_ylabel('CreateLogStream throttling responses')
    for ax in axes:
        ax.set_xlabel('New changing destinations / second')
        ax.set_xticks([30, 60, 100])
        ax.spines[['top', 'right']].set_visible(False)
        ax.legend()
    isolated = load(run / 'preflight.json').get('isolated_collectors_memory_buffers', False)
    subtitle = 'Separate collectors, memory buffers; windows 30s / 60s / 60s' if isolated else 'Shared collector: coupled buffering; some log counts are lower bounds'
    fig.suptitle('Real CloudWatch; emulated collector generations\n' + subtitle, fontsize=11)
    fig.savefig(run / 'demand-comparison.png', dpi=180)
    fig.savefig(run / 'demand-comparison.svg')
    print(json.dumps(audit))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('kind', choices=['lifecycle', 'cloud'])
    parser.add_argument('run')
    args = parser.parse_args()
    (lifecycle if args.kind == 'lifecycle' else cloud)(ROOT / 'results' / args.run)
