# 带压缩历史的消息档案随机取数

frames.py读取真实RFC6455服务端帧，archive.py保持permessage-deflate上下文顺序解码，控制帧插在数据分片中。get(99)仍从消息0开始解压，已有100条真实压缩消息可复现该放大。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程是真实可运行的旧能力，关键新场景尚未实现；fixtures 是候选可用输入，作者检查在题目目录之外。完成后保留可复现的命令与结果说明。
