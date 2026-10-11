"""Recheck the public fleet evidence and plot recorded Fluent Bit counters offline."""
import argparse
from collections import Counter
import gzip
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[1]
RUN = 'kubecon-fleet-20261011-0201'
METRICS = ['fluentbit_input_records_total', 'fluentbit_output_proc_records_total',
           'fluentbit_output_dropped_records_total', 'fluentbit_output_retries_total',
           'fluentbit_output_retries_failed_total', 'fluentbit_output_errors_total']
MODES = ['high-default', 'high-unlimited', 'reused-default']
SAMPLE = re.compile(r'^(\w+)(?:\{([^}]*)\})?\s+([-+\d.eE]+)(?:\s+[-+\d.eE]+)?$')

def parse_metrics(raw):
    """Read the value column, never the optional Prometheus timestamp column."""
    result = {}
    for line in raw.splitlines():
        match = SAMPLE.match(line)
        if not match or match[1] not in METRICS:
            continue
        expected = 'tail.0' if match[1].startswith('fluentbit_input_') else 'cloud'
        labels = dict(re.findall(r'(\w+)="([^"\\]*)"', match[2] or ''))
        if labels.get('name') != expected:
            continue
        if match[1] in result:
            raise ValueError('Duplicate selected metric: ' + match[1])
        result[match[1]] = float(match[3])
    missing = set(METRICS) - result.keys()
    if missing:
        raise ValueError('Missing metrics: ' + ', '.join(sorted(missing)))
    return result

def read(root, name):
    return json.loads(gzip.decompress((root / (name + '.gz')).read_bytes()))

def audit_trial(data):
    terminal = parse_metrics(data['terminal']['raw'])
    previous = None
    valid = 0
    for row in data['observations']:
        if 'raw' not in row:
            continue  # Failed HTTP observations are not zero-valued samples.
        current = parse_metrics(row['raw'])
        if previous and any(current[k] < previous[k] for k in METRICS):
            raise ValueError('Counter reset or non-monotonic observation')
        previous = current
        valid += 1
    if previous and any(terminal[k] < previous[k] for k in METRICS):
        raise ValueError('Terminal counter precedes final observation')
    source = {r['record_id']: r for r in data['source_records']}
    if len(source) != 832 or len(data['source_records']) != 832:
        raise ValueError('Expected 832 unique source records per cluster')
    events = data['backend_events']
    counts = Counter(e['record']['record_id'] for e in events)
    if set(counts) - source.keys() or any(v != 1 for v in counts.values()):
        raise ValueError('Unexpected or duplicate backlog records')
    for event in events:
        if event['record'] != source[event['record']['record_id']]:
            raise ValueError('Record content mismatch')
        expected_stream = 'reused' if 'reused' in data['trial'] else (
            event['record']['source_name'] + '.' + event['record']['source_container'])
        if event['log_stream'] != expected_stream:
            raise ValueError('Unexpected log stream')
    missing = source.keys() - counts.keys()
    if terminal['fluentbit_input_records_total'] != len(source):
        raise ValueError('Input counter differs from source record count')
    if terminal['fluentbit_output_proc_records_total'] != len(counts):
        raise ValueError('Processed counter differs from unique backend IDs')
    if terminal['fluentbit_output_dropped_records_total'] != len(missing):
        raise ValueError('Missing IDs differ from the dropped-record counter')
    after = {e['record']['record_id'] for e in data['events_after_probe']}
    if missing & after:
        raise ValueError('Missing backlog IDs arrived after the recovery probe')
    probes = after - source.keys()
    if len(probes) != 8:
        raise ValueError('Expected eight separate recovery records')
    result = data['recorded_result']
    if result['pending_or_unaccounted'] != 0 or result['unique_delivered'] != len(counts):
        raise ValueError('Recorded result differs from independent reconstruction')
    health = Counter(str(r['health_status']) for r in data['observations'] if 'health_status' in r)
    throttles = Counter()
    for line in data['throttle_log_lines']:
        for api in ['CreateLogStream', 'PutLogEvents']:
            if api in line:
                throttles[api] += 1
    return {'cluster': data['cluster'], 'trial': data['trial'], 'activation': data['activation'],
            'metrics': terminal, 'valid_metric_samples': valid,
            'terminal_seconds_after_activation': data['terminal']['time'] - data['activation'],
            'unique_backend_records': len(counts), 'missing_ids': len(missing), 'probe_records': len(probes),
            'health_observations': dict(health), 'throttling_response_log_lines': dict(throttles),
            'max_activation_to_ingestion_seconds': max(e['ingestion_time_ms']/1000-data['activation'] for e in events)}

def analyze(root):
    index = read(root, 'index.json')
    rows = [audit_trial(read(root, t['file'])) for t in index['trials']]
    if len(rows) != 44:
        raise ValueError('Expected 42 fleet and two solo trial exports')
    fleet = []
    for round_number in [1, 2]:
        for mode in MODES:
            name = f'r{round_number}-{mode}'
            group = [r for r in rows if r['trial'] == name]
            if len(group) != 7 or len({r['activation'] for r in group}) != 1:
                raise ValueError('Incomplete or unsynchronized fleet case')
            totals = {m: sum(r['metrics'][m] for r in group) for m in METRICS}
            fleet.append({'trial': name, 'activation': group[0]['activation'], 'metrics': totals,
                          'terminal_seconds_range': [min(r['terminal_seconds_after_activation'] for r in group),
                                                     max(r['terminal_seconds_after_activation'] for r in group)],
                          'metric_samples': sum(r['valid_metric_samples'] for r in group),
                          'unique_backend_records': sum(r['unique_backend_records'] for r in group),
                          'max_activation_to_ingestion_seconds': max(r['max_activation_to_ingestion_seconds'] for r in group),
                          'health_observations': dict(sum((Counter(r['health_observations']) for r in group), Counter())),
                          'throttling_response_log_lines': dict(sum((Counter(r['throttling_response_log_lines']) for r in group), Counter()))})
    chronological = sorted(fleet, key=lambda f: next(r['activation'] for r in rows if r['trial'] == f['trial']))
    if [r['trial'] for r in chronological] != ['r1-high-default', 'r1-high-unlimited', 'r1-reused-default',
                                             'r2-reused-default', 'r2-high-unlimited', 'r2-high-default']:
        raise ValueError('Unexpected comparison order')
    focal = [next(r for r in rows if r['cluster']=='c01' and r['trial']==name) for name in
             ['r3-high-default', 'r1-high-default', 'r2-high-default', 'r4-high-default']]
    if not focal[0]['activation'] < chronological[0]['activation'] < chronological[-1]['activation'] < focal[-1]['activation']:
        raise ValueError('Baseline order')
    return {'run': RUN, 'failures': [], 'fleet': fleet, 'focal_cluster': focal, 'per_cluster': rows}

def plot(report, output):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.ticker import MaxNLocator
    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10, 'svg.hashsalt': 'fleet-20261011'})
    output.mkdir(parents=True, exist_ok=True)
    rows = report['fleet']
    labels = [r['trial'].replace('r1-', 'Round 1 | ').replace('r2-', 'Round 2 | ')
              .replace('high-default', 'Pod/container + default retries')
              .replace('high-unlimited', 'Pod/container + unlimited retries')
              .replace('reused-default', 'Reused streams + default retries') for r in rows]
    def finish(fig, name):
        fig.savefig(output/(name+'.png'), dpi=170, bbox_inches='tight')
        fig.savefig(output/(name+'.svg'), bbox_inches='tight', metadata={'Date': None})
        svg = output/(name+'.svg')
        svg.write_text('\n'.join(line.rstrip() for line in svg.read_text().splitlines())+'\n')
        plt.close(fig)
    fig, ax = plt.subplots(figsize=(12, 5.2))
    processed = [r['metrics'][METRICS[1]] for r in rows]
    dropped = [r['metrics'][METRICS[2]] for r in rows]
    ax.barh(labels, processed, label='Processed records', color='#287c8e')
    ax.barh(labels, dropped, left=processed, label='Dropped records', color='#bc493e')
    for i, (p, d) in enumerate(zip(processed, dropped)):
        ax.text(p/2, i, f'{p:,.0f}', va='center', ha='center', color='white')
        if d: ax.text(p+d/2, i, f'{d:,.0f}', va='center', ha='center', color='white')
    ax.invert_yaxis(); ax.set_xlim(0, 5824); ax.set_xticks([0, 1000, 2000, 3000, 4000, 5000, 5824])
    ax.set_xlabel('Counter total across seven collectors | 5,824 input records in every case')
    ax.set_title('Output record counters before recovery probes', loc='left', pad=32, fontweight='bold')
    ax.legend(loc='lower left', bbox_to_anchor=(0, 1), ncol=2, frameon=False)
    fig.text(.01, .01, 'fluentbit_output_proc_records_total and fluentbit_output_dropped_records_total; name="cloud"', fontsize=9)
    fig.tight_layout(rect=[0,.06,1,1]);finish(fig, 'fleet-output-records')
    fig, axes = plt.subplots(3,1,figsize=(11,12))
    for ax,key,title,color in zip(axes,METRICS[3:],['Output retries','Exhausted chunk retries','Output errors'],['#287c8e','#bc493e','#65758b']):
        values=[r['metrics'][key] for r in rows]
        ax.barh(labels,values,color=color);span=max(max(values),1)
        ax.set_xlim(0,span*1.2);ax.xaxis.set_major_locator(MaxNLocator(nbins=4,integer=True))
        for i,v in enumerate(values):ax.text(v+span*.02,i,f'{v:,.0f}',va='center')
        ax.invert_yaxis();ax.set_title(title,loc='left',fontweight='bold')
        ax.set_xlabel(key+'  |  Cumulative count',fontsize=10)
    fig.suptitle('Retry and error counters across seven collectors',x=.01,ha='left',fontweight='bold')
    fig.tight_layout(rect=[0,0,1,.96],h_pad=2.5);finish(fig,'fleet-retries-errors')
    fig,ax=plt.subplots(figsize=(10,4.4))
    focal=report['focal_cluster'];values=[r['metrics'][METRICS[2]] for r in focal]
    names=['Alone before','With six other clusters | round 1','With six other clusters | round 2','Alone after']
    ax.barh(names,values,color=['#65758b','#bc493e','#bc493e','#65758b'])
    for i,v in enumerate(values):ax.text(v+4,i,f'{v:,.0f} / 832 ({v/832:.2%})',va='center')
    ax.invert_yaxis();ax.set_xlim(0,325);ax.set_xlabel('Dropped-record counter on c01 | identical 832-record input')
    ax.set_title('The same collector alone and within the fleet',loc='left',fontweight='bold')
    fig.text(.01,.01,'fluentbit_output_dropped_records_total{name="cloud"}; Pod/container names and default retries throughout',fontsize=9)
    fig.tight_layout(rect=[0,.06,1,1]);finish(fig,'fleet-focal-drops')

if __name__ == '__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,default=ROOT/'results'/RUN)
    args=parser.parse_args();report=analyze(ROOT/'evidence'/RUN)
    args.output.mkdir(parents=True,exist_ok=True)
    (args.output/'audit.json').write_text(json.dumps(report,indent=2)+'\n')
    plot(report,args.output)
    print(json.dumps({'fleet_cases':len(report['fleet']),'cluster_trials':len(report['per_cluster']),'failures':report['failures']}))
