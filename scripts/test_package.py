import gzip
import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

def module(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value

verify = module('verify_package', 'verify-package.py').verify
unpack = module('unpack_evidence', 'unpack-evidence.py').unpack
valid_marker = module('cleanup_owned', 'cleanup-owned.py').valid_marker

class PackageTests(unittest.TestCase):
    def test_corruption_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'sample').write_bytes(b'original')
            (root / 'MANIFEST.json').write_text(json.dumps({'files': [{'path': 'sample', 'sha256': hashlib.sha256(b'original').hexdigest()}]}))
            self.assertFalse(verify(root))
            (root / 'sample').write_bytes(b'changed')
            self.assertTrue(verify(root))

    def test_unpack_never_overwrites_different_local_data(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / 'evidence' / 'sample'
            source.mkdir(parents=True)
            (source / 'data.json.gz').write_bytes(gzip.compress(b'{}'))
            self.assertEqual(unpack(root), 1)
            self.assertEqual(unpack(root), 1)
            (root / 'results/sample/data.json').write_text('changed')
            with self.assertRaises(RuntimeError):
                unpack(root)

    def test_cleanup_rejects_unrelated_resources(self):
        run = 'identity-cloud-example'
        marker = {'run': run, 'group': '/kubecon/stream-churn/' + run, 'cluster': 'cwlogs-identity', 'region': 'us-west-2'}
        self.assertTrue(valid_marker(marker, run))
        self.assertFalse(valid_marker({**marker, 'group': '/production'}, run))
        self.assertFalse(valid_marker({**marker, 'cluster': 'production'}, run))

if __name__ == '__main__':
    unittest.main()
