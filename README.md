# 冻结匹配的并发数据合并执行器

planner.py在SQLite快照中给source生成update/insert，executor.py稍后执行。单次合并可用，但冻结计划后另一个写者改了amount，旧计划直接覆盖新值；observed_rev被记录却没有验证。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程是真实可运行的旧能力，关键新场景尚未实现；fixtures 是候选可用输入，作者检查在题目目录之外。完成后保留可复现的命令与结果说明。
