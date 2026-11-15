# 远端变长列的版本一致投影引擎

query.py 通过 HTTP 下载完整 FITS 后交给 astropy，range_server.py 已实现 Range、ETag、If-Match 和指定请求后切换对象；正常结果正确，但投影也下载整文件。

运行基础工程：`python demo.py`。依赖见 requirements.txt；无依赖时使用 Python 3.11 标准库。

基础工程是真实可运行的旧能力，关键新场景尚未实现；fixtures 是候选可用输入，作者检查在题目目录之外。完成后保留可复现的命令与结果说明。
