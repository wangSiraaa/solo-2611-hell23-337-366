# 演进Union列的协商式数据交换

union_reader.py读取真实Arrow dense Union并取出child offset，receiver.py第一次记住type-code映射后一直复用。a批正确，b批交换局部type-code后数值20被错误标成文本分支；当前没有schema协商和受限缓冲。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程是真实可运行的旧能力，关键新场景尚未实现；fixtures 是候选可用输入，作者检查在题目目录之外。完成后保留可复现的命令与结果说明。
