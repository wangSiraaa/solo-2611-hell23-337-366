# 断连与慢消费者下的观测周期采集

nmea.py校验真实NMEA校验和，collector.py按接收器/talker装配，sink.py写SQLite。现有正常路径能出周期，但连接代不进入pending键，断连时旧半周期可拼到新连接；sink没有投递幂等。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程是真实可运行的旧能力，关键新场景尚未实现；fixtures 是候选可用输入，作者检查在题目目录之外。完成后保留可复现的命令与结果说明。
