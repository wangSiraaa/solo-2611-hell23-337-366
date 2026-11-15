import json
from pathlib import Path


def generate(folder, n=100000):
    """Two sources, ~90% of rows sharing one hot key [0, "hot"]."""
    p = Path(folder)
    p.mkdir(parents=True, exist_ok=True)
    for name, offset in [('a', 0), ('b', 1)]:
        with (p / (name + '.jsonl')).open('w') as f:
            for i in range(n):
                f.write(json.dumps([0 if i < n * 9 // 10 else i + offset,
                                    'hot' if i < n * 9 // 10 else 'cold']) + '\n')


if __name__ == '__main__':
    generate('generated')
