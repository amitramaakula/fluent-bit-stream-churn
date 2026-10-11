"""Validate saved AWS/Logs metrics and plot the fleet window without AWS access."""
import argparse
from collections import defaultdict
import datetime as dt
import gzip
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUN = 'kubecon-fleet-20261011-0201'
APIS = ('CreateLogStream', 'PutLogEvents')
METRICS = ('CallCount', 'ThrottleCount')
COLORS = {'CreateLogStream': '#28648c', 'PutLogEvents': '#986636'}


def timestamp(value):
    parsed = dt.datetime.fromisoformat(value)
    if parsed.utcoffset() != dt.timedelta(0):
        raise ValueError('Metric timestamps must include the UTC offset')
    return parsed.timestamp()


def validate_metrics(data):
    if data['run'] != RUN or data['region'] != 'us-west-2':
        raise ValueError('Unexpected experiment or region')
    if data['period_seconds'] != 60 or data['statistic'] != 'Sum':
        raise ValueError('Expected 60-second Sum statistics')
    start, end = timestamp(data['start_inclusive']), timestamp(data['end_exclusive'])
    if start % 60 or end % 60 or end <= start:
        raise ValueError('Window must have minute-aligned boundaries')
    grid = list(range(int(start), int(end), 60))
    series = {}
    for row in data['series']:
        key = (row['api'], row['metric'])
        if key in series or key[0] not in APIS or key[1] not in METRICS:
            raise ValueError('Duplicate or unexpected metric series')
        expected = {'Service': 'Logs', 'Class': 'None', 'Type': 'API', 'Resource': key[0]}
        dims = row['dimensions']
        if (row['namespace'] != 'AWS/Logs' or len(dims) != 4 or
                {d['Name']: d['Value'] for d in dims} != expected):
            raise ValueError('Unexpected namespace or dimensions')
        points = {}
        for point in row['datapoints']:
            when, value = timestamp(point['Timestamp']), point['Sum']
            if when % 60 or not start <= when < end or when in points:
                raise ValueError('Duplicate, unaligned or out-of-window minute')
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError('Invalid count')
            if point['Unit'] not in ('None', 'Count'):
                raise ValueError('Unexpected count unit')
            points[when] = value
        # A returned zero and an absent minute are kept distinct.
        series[key] = [points.get(when) for when in grid]
    if set(series) != {(a, m) for a in APIS for m in METRICS}:
        raise ValueError('Missing series, including empty series')
    return grid, series


def read(root, name):
    return json.loads(gzip.decompress((root / (name + '.gz')).read_bytes()))


def trial_windows(root):
    grouped = defaultdict(list)
    for item in read(root, 'index.json')['trials']:
        trial = read(root, item['file'])
        if (trial['trial'], trial['cluster']) != (item['trial'], item['cluster']):
            raise ValueError('Trial index mismatch')
        grouped[trial['trial']].append(trial)
    windows = []
    expected = {f'r{r}-{m}': 7 for r in (1, 2)
                for m in ('high-default', 'high-unlimited', 'reused-default')}
    expected.update({'r3-high-default': 1, 'r4-high-default': 1})
    if set(grouped) != set(expected):
        raise ValueError('Unexpected trial set')
    for name, trials in grouped.items():
        starts = {t['activation'] for t in trials}
        clusters = {t['cluster'] for t in trials}
        required_clusters = {'c01'} if expected[name] == 1 else {f'c{i:02}' for i in range(1, 8)}
        if len(trials) != expected[name] or clusters != required_clusters or len(starts) != 1:
            raise ValueError('Incomplete or unsynchronized comparison')
        start = starts.pop()
        if any(t['terminal']['time'] < start for t in trials):
            raise ValueError('Terminal sample precedes activation')
        windows.append({'trial': name, 'clusters': len(trials), 'start': start,
                        'end': max(t['terminal']['time'] for t in trials)})
    return sorted(windows, key=lambda w: w['start'])


def draw(data, grid, series, windows, out):
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.ticker import StrMethodFormatter

    plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                         'svg.hashsalt': 'fleet-api-20261011'})
    x = [mdates.date2num(dt.datetime.fromtimestamp(t, dt.timezone.utc)) for t in grid]
    minute = 1 / 1440
    for metric, suffix, heading, unit in [
        ('CallCount', 'calls', 'CloudWatch Logs API calls', 'Calls per minute'),
        ('ThrottleCount', 'throttles', 'CloudWatch Logs API throttling', 'Throttled requests per minute')]:
        fig, axes = plt.subplots(3, 1, figsize=(12, 8), sharex=True,
                                 gridspec_kw={'height_ratios': [2.1, 2.1, 1.55]})
        fig.subplots_adjust(left=.10, right=.975, top=.865, bottom=.175, hspace=.48)
        fig.suptitle(heading, x=.10, ha='left', y=.97, fontsize=19, weight='bold')
        fig.text(.10, .925, f'AWS/Logs · {metric} · 60-second Sum · 11 October 2026 · us-west-2',
                 color='#43505d', fontsize=11)
        for ax, api in zip(axes[:2], APIS):
            values = series[(api, metric)]
            present = [(when, value) for when, value in zip(x, values) if value is not None]
            ax.set_title(api, loc='left', fontsize=12, weight='bold', pad=9)
            ax.spines[['top', 'right']].set_visible(False)
            if present:
                ax.bar([p[0] for p in present], [p[1] for p in present], width=minute,
                       align='edge', color=COLORS[api], edgecolor='white', linewidth=.45, zorder=3)
                ax.set_ylim(bottom=0)
                ax.yaxis.set_major_formatter(StrMethodFormatter('{x:,.0f}'))
                ax.set_ylabel(unit, fontsize=9)
                ax.grid(axis='y', color='#e4e7ea', zorder=0)
                # Zero samples, if present, are visible rather than mistaken for missing data.
                zero_x = [when + minute / 2 for when, value in present if value == 0]
                if zero_x:
                    ax.plot(zero_x, [0] * len(zero_x), 'o', color=COLORS[api], markersize=3, clip_on=False)
                for when, value in zip(x, values):
                    if value is None:
                        ax.axvspan(when, when + minute, facecolor='#f1f2f3',
                                   edgecolor='#c5cbd0', hatch='///', linewidth=0, zorder=1)
                ax.text(1, 1.07, f'{len(present)}/{len(grid)} minutes reported · sum {sum(v for _, v in present):,.0f}',
                        transform=ax.transAxes, ha='right', fontsize=10, color='#43505d')
            else:
                ax.set_facecolor('#f1f2f3')
                ax.set_yticks([])
                ax.spines[['left', 'bottom']].set_visible(False)
                ax.text(.5, .55, 'No ThrottleCount datapoints were returned',
                        transform=ax.transAxes, ha='center', fontsize=13)
                ax.text(.5, .30, 'An empty series is not a measured zero.',
                        transform=ax.transAxes, ha='center', color='#43505d')

        lane = axes[2]
        labels = ['Default retries', 'Unlimited retries', 'Reused streams', 'c01 alone']
        lane.set_title('Trial windows: 7 clusters unless marked c01 alone; numbers identify the round',
                       loc='left', fontsize=10, pad=11)
        for w in windows:
            if w['clusters'] == 1:
                row, color, label = 3, '#9da5ae', 'B' if w['trial'].startswith('r3') else 'A'
            elif 'reused' in w['trial']:
                row, color, label = 2, '#78ad93', w['trial'][1]
            elif 'unlimited' in w['trial']:
                row, color, label = 1, '#d8a261', w['trial'][1]
            else:
                row, color, label = 0, '#798db3', w['trial'][1]
            start = mdates.date2num(dt.datetime.fromtimestamp(w['start'], dt.timezone.utc))
            end = mdates.date2num(dt.datetime.fromtimestamp(w['end'], dt.timezone.utc))
            lane.broken_barh([(start, end-start)], (row-.32, .64), facecolors=color)
            lane.text((start+end)/2, row, label, ha='center', va='center', fontsize=9, weight='bold')
        lane.set_yticks(range(4), labels, fontsize=9)
        lane.set_ylim(3.55, -.55)
        lane.spines[['left', 'right', 'top']].set_visible(False)
        lane.tick_params(axis='y', length=0)
        lane.xaxis.set_major_locator(mdates.MinuteLocator(byminute=range(0, 60, 5)))
        lane.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M', tz=dt.timezone.utc))
        lane.set_xlim(x[0], x[-1]+minute)
        lane.set_xlabel('UTC · B = before fleet trials; A = after fleet trials', fontsize=10, labelpad=9)
        fig.text(.10, .07, 'Bars cover one minute. Hatched gaps have no returned sample; gaps were not filled with zeros.',
                 fontsize=10, color='#43505d')
        fig.text(.10, .04, 'Account/region totals include recovery probes and other API activity. Boundary minutes can span trial phases.',
                 fontsize=10, color='#43505d')
        for ext in ('png', 'svg'):
            target = out / f'fleet-api-{suffix}.{ext}'
            fig.savefig(target, dpi=180, metadata={'Creator': 'analyze-fleet-api.py', 'Date': None}
                        if ext == 'svg' else {'Software': 'analyze-fleet-api.py'})
            if ext == 'svg':
                target.write_bytes(('\n'.join(line.rstrip() for line in target.read_text(encoding='utf-8').splitlines()) + '\n').encode())
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--evidence', type=Path, default=ROOT / 'evidence' / RUN)
    parser.add_argument('--out', type=Path, default=ROOT / 'results' / RUN)
    args = parser.parse_args()
    data = read(args.evidence, 'aws-api-metrics.json')
    grid, series = validate_metrics(data)
    windows = trial_windows(args.evidence)
    if any(w['start'] < grid[0] or w['end'] > grid[-1]+60 for w in windows):
        raise ValueError('Trial window falls outside the metric query')
    args.out.mkdir(parents=True, exist_ok=True)
    report = {'run': RUN, 'fetched_at_utc': data['fetched_at_utc'], 'scope': data['scope'],
              'start_inclusive': data['start_inclusive'], 'end_exclusive': data['end_exclusive'],
              'trial_windows': windows,
              'series': [{'api': api, 'metric': metric, 'reported_minutes': sum(v is not None for v in vals),
                          'missing_minutes': sum(v is None for v in vals),
                          'sum_of_returned_points': sum(v for v in vals if v is not None) if any(v is not None for v in vals) else None}
                         for (api, metric), vals in series.items()]}
    (args.out / 'api-metric-audit.json').write_text(json.dumps(report, indent=2) + '\n')
    draw(data, grid, series, windows, args.out)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
