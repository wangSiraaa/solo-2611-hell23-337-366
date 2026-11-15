# 增长波形源的稳定版本数据索引

vcd.py 能解释封口文件的信号变更，index.py 全量重建SQLite事件表后查询。已有源包含同tick多次赋值、别名、dumpoff/dumpon；它把尚未写完的token直接交给解析器，也没有公开稳定视图的边界。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程是真实可运行的旧能力，关键新场景尚未实现；fixtures 是候选可用输入，作者检查在题目目录之外。完成后保留可复现的命令与结果说明。
