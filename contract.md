time_rules.py能做普通UTC/TAI转换，association.py产生两源同key同刻配对，materializer.py存物化结果；换闰秒表只换metadata，关联事实仍旧。请升级为ingest、install-rules、query、explain组成的事件数据加工目录，支持规则修订和可复核结果撤回，不做NTP/时钟校准算法。

使用legacy/leaps.json、legacy/events.json及旧题明确的正闰秒与纳秒契约，补齐UTC :60、TAI和GPS归一，整数纳秒且不经浮点。新规则表声明id、coverage、sealed_before及offset项；新旧表在sealed_before之前必须相同，之后的暂定覆盖可更正/延伸。事件最多100000、规则3版、保留原始表示与所用规则。TAI/GPS绝对身份不随UTC规则改变；UTC事件可能移动、由unsupported转可用或反向失效，不能把旧tai当永恒真值。

关联规则固定：A和B按key做全部bag区间匹配，|taiA-taiB|<=tolerance_ns；每对身份是(A_id,B_id)，不同id同一时刻不去重。窗口汇总按A的tai落入半开宽度窗口，统计配对数与A/B来源集合；窗口origin和width在配置固定，tolerance<=width。规则修订必须联合产生规范事件净变更、配对insert/retract及窗口count delta，外部消费者重放delta后与当前query完全相等；同对撤回后再插入要可解释，不得以净数抵消隐藏身份变化。

需要决定原始事件索引、规则影响范围索引和配对物化方式，比较全量重算与局部重算对于边界移动、失效事件及旧版本查询的影响。允许全量独立oracle；正式更新的规范化审计只包含规则可能改变的UTC事件（不能把全部TAI/GPS重新标为改变），配对重算须覆盖这些事件在新旧位置的邻域。可以读未变事件作邻居，必须分开计量读取与结果变化。提供按规则版本查最近两版结果；规则安装失败不能暴露一半新规范时间和一半旧配对。

验证正闰秒前/中/后纳秒、等价TAI/GPS、暂定表补闰秒、覆盖延伸释放事件、非法表/已封存历史变化拒绝、关联从0变1及1变0、A跨窗口边界、两个事件同时移动而pair身份不变、关联更新前注入异常。真实读原始事件并重建每版oracle，逐配对及窗口值比较，验证delta可被独立小消费者重放。交付源码、规则/索引设计、真实变更日志；不做DST、负闰秒、未来真实闰秒预测、通用流平台或分布式时钟。
