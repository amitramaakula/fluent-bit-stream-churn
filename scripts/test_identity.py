"""Order controls must preserve design/port assignment and record-audit failures."""
import importlib.util
from pathlib import Path
import unittest

def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value

runner = module('identity_cloud', 'identity-cloud.py')
analysis = module('identity_analysis', 'analyze-identity.py')

class OrderTests(unittest.TestCase):
    def test_reversal_preserves_design_port_mapping(self):
        normal = runner.deployment_order(True)
        reverse = runner.deployment_order(True, True)
        self.assertEqual(reverse, list(reversed(normal)))
        self.assertEqual(dict(normal), dict(reverse))
        self.assertEqual([23201 + i * 2 for i, _ in reverse], runner.endpoint_order(True, True))

    def test_normal_and_shared_endpoint_order(self):
        self.assertEqual(runner.endpoint_order(True), [23201, 23203])
        self.assertEqual(runner.endpoint_order(False), [23201])

    def test_percentile_is_nearest_rank(self):
        self.assertEqual(analysis.percentile(list(range(1, 101)), .95), 95)
        self.assertEqual(analysis.percentile([5], .95), 5)
        self.assertIsNone(analysis.percentile([], .95))

if __name__ == '__main__':
    unittest.main()
