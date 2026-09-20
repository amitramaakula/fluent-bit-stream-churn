"""Expand text evidence without overwriting a different local result."""
import gzip
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def unpack(root):
    count = 0
    for source in sorted((root / 'evidence').glob('*/*.gz')):
        target = root / 'results' / source.parent.name / source.name[:-3]
        content = gzip.decompress(source.read_bytes())
        if target.exists() and target.read_bytes() != content:
            raise RuntimeError(f'Refusing to overwrite changed evidence: {target}')
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        count += 1
    return count

if __name__ == '__main__':
    print(f'Expanded {unpack(ROOT)} evidence files')
