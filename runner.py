import json
from pathlib import Path
from operators import evaluate
def run(plan,paths):
    sources={k:[json.loads(line) for line in Path(p).read_text().splitlines()] for k,p in paths.items()}
    return evaluate(plan,sources)
