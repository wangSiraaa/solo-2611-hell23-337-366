# 媒体时间覆盖驱动的有预算离线采集发布

可验证的离线片段数据管道：`plan` / `collect` / `verify` 三阶段，主需求以
contract.md 为准，设计取舍见 DESIGN.md。legacy/ 是旧格式示例与旧实现存档，
不参与新管线。

## 运行

Python 3.11，仅标准库（sqlite3 等），无第三方依赖：

```sh
python -m pip install -r requirements.txt
PYTHONDONTWRITEBYTECODE=1 python3 baseline_demo.py        # 双修订场景演示（旧缺口已修复）
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tests -v
```

`baseline_demo.py` 成功退出表示新管线在旧出问题的场景上行为正确；旧缺陷的
历史复现保留在 `legacy/original_impl/baseline_demo_original.py`。

## 用法（路径均相对当前仓库根目录）

```sh
# 只出计划与证据（不读 Origin 对象字节）
python3 pipeline.py plan --manifest inputs/r2.json --request inputs/request.json --store store/

# 采集并原子发布（--strategy global|online，默认 global）
python3 pipeline.py collect --manifest inputs/r2.json --request inputs/request.json \
    --origin origin/ --store store/

# 独立核验当前/指定发布，或核验一条失败记录
python3 pipeline.py verify --store store/ --publication current
python3 pipeline.py verify --store store/ --failure fail-xxxxxxxxxxxxxxxx
```

退出码：`plan`/`verify` 成功 0；`collect` 发布成功 0、不可交付 3；核验失败 1。

## 交付物

- 实际字节缓存：`store/objects/<sha256>` + sqlite 索引（身份 = url+revision）。
- 可重读离线片段清单：`store/publications/<id>/manifest.json`（含源清单快照、
  原寻址段表、覆盖/成本证据、决策事件）；失败记录在 `store/failures/`。
- 验证：`verify` 重读发布，逐对象核摘要、逐段核时间映射，并用独立枚举
  oracle 对照决策；测试见 `tests/test_pipeline.py`（全部真实读字节）。
