"""Audit regressions: missing, duplicated or misattributed records must be visible."""
import json
import unittest
from rollout import audit


class RecordAuditTests(unittest.TestCase):
    def setUp(self):
        self.expected = {'uid:0': {'uid': 'uid', 'name': 'checkout-old'}}
        body = {'record_id': 'uid:0', 'source_uid': 'uid',
                'kubernetes': {'pod_id': 'uid', 'pod_name': 'checkout-old'}}
        self.events = [{'logStreamName': name, 'message': json.dumps(body)}
                       for name in ['pod-checkout-old', 'reused-logs-00']]

    def test_missing_is_not_complete(self):
        result = audit(self.events[:1], self.expected)
        self.assertEqual(result['designs']['reused']['missing_records'], 1)

    def test_duplicate_does_not_mask_missing(self):
        expected = {**self.expected, 'uid:1': self.expected['uid:0']}
        result = audit(self.events + self.events, expected)
        self.assertEqual(result['designs']['pod']['missing_records'], 1)
        self.assertEqual(result['designs']['pod']['extra_deliveries'], 1)

    def test_wrong_metadata_fails(self):
        body = json.loads(self.events[0]['message'])
        body['kubernetes']['pod_id'] = 'different-pod'
        self.events[0]['message'] = json.dumps(body)
        self.assertTrue(audit(self.events, self.expected)['metadata_errors'])

    def test_fallback_stream_fails(self):
        self.events[0]['logStreamName'] = 'ERROR-pod-fallback'
        self.assertTrue(audit(self.events, self.expected)['metadata_errors'])

    def test_complete_delivery(self):
        result = audit(self.events, self.expected)
        self.assertFalse(result['metadata_errors'])
        self.assertTrue(all(d['missing_records'] == 0 for d in result['designs'].values()))


if __name__ == '__main__':
    unittest.main()
