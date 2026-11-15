# 历史复现：升级前的原始基线演示（保留作对照，不参与新管线）。
# 运行：PYTHONDONTWRITEBYTECODE=1 python3 legacy/original_impl/baseline_demo_original.py
# 它复现三个已知缺口：按 URL 复用缓存（r2 引用 r1 的 init）、下载全部表示、
# 最终清单引用旧初始化段。退出码 0 表示“旧缺陷复现正确”。
from pathlib import Path
import json, sys, tempfile
HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
from collector import collect
from origin import Origin
with tempfile.TemporaryDirectory() as t:
    o = Origin(ROOT / 'origin')
    collect(ROOT / 'inputs/r1.json', o, Path(t) / 'cache', Path(t) / 'manifest.json')
    before = o.bytes
    collect(ROOT / 'inputs/r2.json', o, Path(t) / 'cache', Path(t) / 'manifest.json')
    stale = (Path(t) / 'cache/init.bin').read_bytes() != (ROOT / 'origin/r2/init.bin').read_bytes()
    assert stale and o.bytes == before
    assert (ROOT / 'origin/r1/low-0.bin').read_bytes() != (ROOT / 'origin/r2/low-0.bin').read_bytes()
    print(json.dumps({'baseline_runs': 2, 'actual_origin_reads': o.log,
                      'stale_cache_reproduced': stale, 'budget': 40,
                      'low_bundle_bytes': sum(len((ROOT / 'origin/r1' / x).read_bytes())
                                              for x in ['init.bin', 'low-0.bin', 'low-1.bin'])}))
