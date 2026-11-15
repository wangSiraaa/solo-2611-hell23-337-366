import json
from pathlib import Path
from runner import run
print(json.dumps(run(json.loads(Path('fixtures/plan.json').read_text()),{'a':'fixtures/a.jsonl','b':'fixtures/b.jsonl'})))
