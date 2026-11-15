# 时空目录与可验证资产副本的联合取数

在可运行基线之上实现的完整工程：把本地 STAC 1.0 受限子集构建为**可脱离源
JSON 的 SQLite 目录**，再以固定 build 版本做空间/时间查询计划，最终交付带
逐块或整文件验真的**实际资产字节包**。所有读取都是本地真实文件，无需外网
账号；不做栅格解码、复杂几何、云认证或前端。

## 命令（均在仓库根目录执行，路径相对根目录）

```bash
python make_fixtures.py     # 一次性生成确定性夹具 fixtures/

# 1) build：STAC + 资产清单 -> 不可变 SQLite 目录（内容寻址 build_id）
python cli.py build \
  --stac-root fixtures/catalog --replica-root fixtures \
  --manifest fixtures/manifests/snapshot-v1.json \
  --db out/catalog.db --verify full --out out/build-report.json

# 2) plan-query：bbox 为 w,s,e,n；w>e 自动拆日期线；时间为 start/end（闭区间）
python cli.py plan-query --db out/catalog.db \
  --bbox 179.5,-2,-179.5,2 \
  --time 2026-01-01T00:00:00Z/2026-01-01T00:00:00Z \
  --build <build_id> --plan-id wrap --out out/plan.json
# 可选范围取数：--ranges item:asset_key=start-end;...
# 负值参数请用 --bbox=-0.5,... 形式，避免被当成选项

# 3) fetch-query：按固定计划取数验真，写字节包与 manifest.json
python cli.py fetch-query --plan out/plan.json \
  --replica-root fixtures --cache out/cache --package out/pkg --plan-id wrap
# 完整包退出码 0；存在失败资产时包标记 complete=false 且退出码 2

# 4) inspect：只重读 SQLite / 包清单（不访问源 JSON 与原始副本）
python cli.py inspect catalog --db out/catalog.db --build <build_id>
python cli.py inspect package --package out/pkg
```

## 模块

`catalog.py`（发现/校验/精确谓词，保留基线函数）· `manifest.py`（身份与副本
清单）· `replica.py`（本地真实文件适配器与故障注入，保留基线 `Origin`）·
`store.py`（版本化 SQLite 与空间候选索引）· `planner.py`（固定版本查询计划）·
`cache.py`（内容寻址已验真缓存、原子发布、引用回收）· `fetcher.py`（选择策略、
副本回退、按块/整文件验真、不完整包）· `inspect_cmd.py`（重读产物）。

设计与选择策略见 [DESIGN.md](DESIGN.md)。

## 验证

```bash
python verify_pipeline.py     # 82 项断言；空间 oracle + 字节 oracle，退出码 0
```

覆盖：跨日期线 bbox 假命中、闭时间端点、重复 Item 路径、身份冲突、坏副本回退、
range 跨块合并、整文件型副本不伪装范围、同内容共享与来源保留、并发半写缓存、
build 版本固定、同 URL 换内容、计划释放回收、SQLite 与包清单重读。真实日志写
入 `out/verification-log.json`。

## 基线兼容

`python baseline_demo.py` 行为保持不变，仍复现“日期线 Item 误命中地球中部 +
读到陈旧副本”的已知缺口；新管线正是对该缺口的修复（精确部件过滤 + 内容验真）。
