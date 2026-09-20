"""Exercise corrupted and incomplete evidence, not just happy-path totals."""
import contextlib
import copy
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('cloud_analysis', Path(__file__).with_name('analyze-identity.py'))
analysis = importlib.util.module_from_spec(spec)
spec.loader.exec_module(analysis)

class CloudAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.addCleanup(analysis.plt.close, 'all')
        self.root = Path(self.tmp.name)
        self.expected = {str(i): {'record_id': str(i), 'stage': 'rate-30', 'created_ms': 100000,
                         'high_stream': 'high-' + str(i), 'reused_stream': 'reused-' + str(i)} for i in range(2)}
        self.events = [{'eventId': design + rid, 'message': json.dumps(record), 'logStreamName': record[design + '_stream'],
                        'ingestionTime': 100500} for rid, record in self.expected.items() for design in ['high', 'reused']]
        self.write('expected-records.json', self.expected)
        self.write('stages.json', [{'stage': 'rate-30', 'start': 100, 'send_end': 130, 'duration_seconds': 30,
                                   'drain_observed_complete': 140, 'expected_per_design': 2}])
        log = '[1970/01/01 00:01:39.000000] [info] [output:cloudwatch_logs:high] ready\n'
        (self.root / 'final-fluent.log').write_text(log)
        (self.root / 'fluent-high-live.log').write_text(log)
        (self.root / 'monitor.jsonl').write_text('')
        self.write('preflight.json', {'isolated_collectors_memory_buffers': True})

    def write(self, filename, data):
        (self.root / filename).write_text(json.dumps(data))

    def run_audit(self):
        self.write('cloudwatch-events.json', self.events)
        with contextlib.redirect_stdout(io.StringIO()):
            analysis.cloud(self.root)
        return json.loads((self.root / 'audit.json').read_text())

    def test_duplicate_does_not_mask_missing(self):
        self.events = [e for e in self.events if e['eventId'] != 'high1']
        duplicate = copy.deepcopy(self.events[0])
        duplicate['eventId'] = 'another-delivery'
        self.events.append(duplicate)
        result = self.run_audit()
        self.assertFalse(result['all_unique_records_delivered'])
        high = next(row for row in result['rows'] if row['design'] == 'high')
        self.assertEqual((high['missing'], high['duplicates']), (1, 1))

    def test_changed_payload_is_rejected(self):
        body = json.loads(self.events[0]['message'])
        body['created_ms'] = 999999
        self.events[0]['message'] = json.dumps(body)
        with self.assertRaisesRegex(AssertionError, 'Payload mismatch'):
            self.run_audit()

    def test_empty_run_is_not_success(self):
        self.write('stages.json', [])
        with self.assertRaisesRegex(AssertionError, 'Empty or incomplete'):
            self.run_audit()

    def test_repeated_reader_event_id_is_rejected(self):
        self.events.append(copy.deepcopy(self.events[0]))
        with self.assertRaises(AssertionError):
            self.run_audit()

if __name__ == '__main__':
    unittest.main()
