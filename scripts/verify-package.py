"""Check the release inventory and its immutable file hashes without cloud access."""
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def verify(root):
    manifest = json.loads((root / 'MANIFEST.json').read_text())
    errors = []
    for entry in manifest['files']:
        path = root / entry['path']
        if not path.resolve().is_relative_to(root.resolve()) or not path.is_file():
            errors.append(entry['path'] + ': missing or outside package')
        elif hashlib.sha256(path.read_bytes()).hexdigest() != entry['sha256']:
            errors.append(entry['path'] + ': hash mismatch')
    return errors

if __name__ == '__main__':
    errors = verify(ROOT)
    if errors:
        raise SystemExit('\n'.join(errors))
    print('Package file hashes verified')
