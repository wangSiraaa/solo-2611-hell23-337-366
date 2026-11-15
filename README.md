# 预报统计区间目录与一致周期覆盖查询

这是可运行但存在已说明缺口的工程起点。主需求以 contract.md 为准；legacy 是旧格式示例，其旧范围不能覆盖本次新要求。

使用 Python 3.11：`python -m pip install -r requirements.txt`，然后在当前仓库根目录执行 `PYTHONDONTWRITEBYTECODE=1 python baseline_demo.py`。它会真实运行现有模块、显示一个已知缺陷；成功退出表示复现正确，不表示新目标已完成。

所有输入路径相对当前仓库根目录。候选需实现 contract.md 全部范围并自行补验证，不能只修改演示打印。
