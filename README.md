# 可回查重复项的外存集合执行器

`exmem/` 是可溢写的外存引擎：UNION ALL / INTERSECT ALL / EXCEPT ALL 按 AST 原括号
语义执行，每个出现携带 `AST扫描路径:源:行号` 见证；等值组内按左见证顺序消耗配额；
常驻行 payload ≤4096 条（计量器强制），临时磁盘全程计量、中间结果用完即删。
`operators.py` 保留为内存参考实现，用于差分验证；`make_large.py` 生成每源 10 万行、
90% 单热点键的基准输入。

运行基础工程：`python3 demo.py`（走新引擎，输出与旧基线一致）。无第三方依赖，
使用 Python 3.11 标准库。

常用命令：

```bash
python3 -m unittest discover -s tests     # 不变量测试
python3 -m exmem bench                    # 热点资源基准（10万行/源，6x 磁盘与 4096 校验）
python3 -m exmem run --plan fixtures/plan.json --source a=fixtures/a.jsonl \
    --source b=fixtures/b.jsonl --out out.jsonl --workdir tmp --report report.json
python3 experiments/hash_vs_sort.py       # 外部排序 vs 分区哈希的取舍实验
```

设计、实测数据与未覆盖边界见 `REPORT.md`。
