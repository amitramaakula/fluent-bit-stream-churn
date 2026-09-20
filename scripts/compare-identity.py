"""Compare completed isolated sweeps without pooling their distributions."""
import argparse
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('repeat')
    args = p.parse_args()
    root = Path(__file__).resolve().parents[1]
    ids = ['identity-cloud-20260915T022435Z', args.repeat]
    audits = [json.loads((root / 'results' / run / 'audit.json').read_text()) for run in ids]
    rows = []
    for run, audit in zip(ids, audits):
        for rate in [30, 60, 100]:
            high, reused = [next(r for r in audit['rows'] if r['stage'] == f'rate-{rate}' and r['design'] == d) for d in ['high', 'reused']]
            rows.append({'run': run, 'rate': rate, 'changing_throttles': high['creation_throttling_responses'],
                         'reused_throttles': reused['creation_throttling_responses'],
                         'changing_p95_s': high['p95_first_ingestion_seconds'], 'reused_p95_s': reused['p95_first_ingestion_seconds'],
                         'p95_reduction_percent': 100 * (1 - reused['p95_first_ingestion_seconds'] / high['p95_first_ingestion_seconds']),
                         'missing': high['missing'] + reused['missing'], 'duplicates': high['duplicates'] + reused['duplicates']})
    folder = root / 'results' / args.repeat
    (folder / 'repeat-comparison.json').write_text(json.dumps(rows, indent=2))
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.5), layout='constrained')
    for idx, (run, audit) in enumerate(zip(ids, audits)):
        for design, color in [('high', '#bb493c'), ('reused', '#167875')]:
            data = [r for r in audit['rows'] if r['design'] == design]
            xs = [int(r['stage'].split('-')[1]) for r in data]
            label = ('Changing' if design == 'high' else 'Reused') + (' / original' if idx == 0 else ' / reversed order')
            style = 'o-' if idx == 0 else 's--'
            axes[0].plot(xs, [r['p95_first_ingestion_seconds'] for r in data], style, color=color, label=label)
            axes[1].plot(xs, [r['creation_throttling_responses'] for r in data], style, color=color, label=label)
    axes[0].set_ylabel('p95 first-ingestion delay (seconds)')
    axes[1].set_ylabel('CreateLogStream throttling responses')
    for ax in axes:
        ax.set_xlabel('New changing names / second')
        ax.set_xticks([30, 60, 100])
        ax.spines[['top', 'right']].set_visible(False)
        ax.legend(fontsize=8)
    fig.suptitle('Two isolated CloudWatch sweeps; identical stage schedule\nStartup and per-record endpoint order reversed; counts include drain', fontsize=11)
    for ext in ['png', 'svg']:
        fig.savefig(folder / ('repeat-comparison.' + ext), dpi=180)
    print(json.dumps(rows))

if __name__ == '__main__':
    main()
