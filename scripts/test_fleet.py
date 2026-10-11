import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location('fleet', Path(__file__).with_name('analyze-fleet.py'))
fleet = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fleet)

class FleetMetricsTests(unittest.TestCase):
    def samples(self):
        return '\n'.join(f'{metric}{{name="'+('tail.0' if metric.startswith('fluentbit_input_') else 'cloud')+'"} 7 1791688003779' for metric in fleet.METRICS)

    def test_optional_timestamp_is_not_metric_value(self):
        self.assertEqual(set(fleet.parse_metrics(self.samples()).values()), {7.0})

    def test_timestamp_free_format_is_supported(self):
        self.assertEqual(set(fleet.parse_metrics(self.samples().replace(' 1791688003779','')).values()), {7.0})

    def test_missing_sample_is_not_silently_zero(self):
        with self.assertRaisesRegex(ValueError,'Missing metrics'):
            fleet.parse_metrics('\n'.join(self.samples().splitlines()[1:]))

    def test_unrelated_output_is_not_added(self):
        extra='fluentbit_output_errors_total{name="another-output"} 999 1791688003779'
        self.assertEqual(fleet.parse_metrics(self.samples()+'\n'+extra)['fluentbit_output_errors_total'],7)

    def test_duplicate_selected_series_is_rejected(self):
        with self.assertRaisesRegex(ValueError,'Duplicate'):
            fleet.parse_metrics(self.samples()+'\n'+self.samples().splitlines()[0])

if __name__ == '__main__':
    unittest.main()
