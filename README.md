# 可回查重复项的外存集合执行器

operators.py已实现小数据的UNION ALL/INTERSECT ALL/EXCEPT ALL并带AST扫描出现身份；runner.py一次性读入全部源和所有中间结果。make_large.py能产生真实90%单热点键的输入，当前结构必然超出指定行载荷内存界限。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程是真实可运行的旧能力，关键新场景尚未实现；fixtures 是候选可用输入，作者检查在题目目录之外。完成后保留可复现的命令与结果说明。
