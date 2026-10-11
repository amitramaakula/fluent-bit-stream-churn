import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('fleet_api', Path(__file__).with_name('analyze-fleet-api.py'))
api = importlib.util.module_from_spec(spec)
spec.loader.exec_module(api)


def fixture():
    data = {'run': api.RUN, 'region': 'us-west-2', 'period_seconds': 60, 'statistic': 'Sum',
            'start_inclusive': '2026-10-11T03:00:00+00:00',
            'end_exclusive': '2026-10-11T03:02:00+00:00', 'series': []}
    for name in api.APIS:
        for metric in api.METRICS:
            data['series'].append({'namespace': 'AWS/Logs', 'api': name, 'metric': metric,
                                   'dimensions': [{'Name': k, 'Value': v} for k, v in
                                                  {'Service': 'Logs', 'Class': 'None', 'Type': 'API', 'Resource': name}.items()],
                                   'datapoints': []})
    return data


class ApiMetricTests(unittest.TestCase):
    def test_empty_series_remains_missing(self):
        grid, series = api.validate_metrics(fixture())
        self.assertEqual(len(grid), 2)
        self.assertEqual(series[('PutLogEvents', 'ThrottleCount')], [None, None])

    def test_real_zero_is_distinct_and_timestamp_controls_position(self):
        data = fixture()
        data['series'][0]['datapoints'] = [{'Timestamp': '2026-10-11T03:01:00+00:00', 'Sum': 0, 'Unit': 'None'}]
        _, series = api.validate_metrics(data)
        self.assertEqual(series[('CreateLogStream', 'CallCount')], [None, 0])

    def test_duplicate_or_wrong_namespace_rejected(self):
        for wrong in ('duplicate', 'namespace', 'dimension', 'dimension_duplicate'):
            data = fixture()
            if wrong == 'duplicate':
                data['series'].append(copy.deepcopy(data['series'][0]))
            elif wrong == 'namespace':
                data['series'][0]['namespace'] = 'AWS/Usage'
            elif wrong == 'dimension':
                data['series'][0]['dimensions'][-1]['Value'] = 'PutLogEvents'
            else:
                data['series'][0]['dimensions'].append({'Name': 'Service', 'Value': 'Logs'})
            with self.subTest(wrong=wrong), self.assertRaises(ValueError):
                api.validate_metrics(data)

    def test_invalid_minute_and_count_rejected(self):
        for when, value in [('2026-10-11T03:00:01+00:00', 1),
                            ('2026-10-11T03:02:00+00:00', 1),
                            ('2026-10-11T03:00:00', 1),
                            ('2026-10-11T03:00:00+00:00', -1),
                            ('2026-10-11T03:00:00+00:00', float('nan'))]:
            data = fixture()
            data['series'][0]['datapoints'] = [{'Timestamp': when, 'Sum': value, 'Unit': 'None'}]
            with self.subTest(when=when, value=value), self.assertRaises(ValueError):
                api.validate_metrics(data)

    def test_duplicate_minute_rejected(self):
        data = fixture()
        point = {'Timestamp': '2026-10-11T03:00:00+00:00', 'Sum': 1, 'Unit': 'None'}
        data['series'][0]['datapoints'] = [point, copy.deepcopy(point)]
        with self.assertRaises(ValueError):
            api.validate_metrics(data)

    def test_missing_series_rejected_even_if_it_was_empty(self):
        data = fixture()
        data['series'].pop()
        with self.assertRaises(ValueError):
            api.validate_metrics(data)


if __name__ == '__main__':
    unittest.main()
