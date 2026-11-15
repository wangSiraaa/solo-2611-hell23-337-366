# 十进制列仓的分片在线语义迁移

shards.py读取真实decimal Parquet并聚合，migration.py把单分片cast到目标类型。safe示例能转换，但safe=False在缩scale时截断，未迁移分片的target查询又先求和后round，两条路径会产生不同金额。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程是真实可运行的旧能力，关键新场景尚未实现；fixtures 是候选可用输入，作者检查在题目目录之外。完成后保留可复现的命令与结果说明。
