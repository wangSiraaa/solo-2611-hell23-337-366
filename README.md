# 断连与慢消费者下的观测周期采集

在基线(nmea.py 校验和、collector.py 装配、sink.py 写 SQLite)之上完成升级:
按连接代装配 GSV 周期、持久投递账本、幂等下游、有界背压、崩溃恢复与确定性演示。
运行时仅依赖 Python 3.11 标准库;测试需要 pytest(`pip install pytest`)。

## 模块

| 文件 | 职责 |
|---|---|
| `nmea.py` | GSV 严格校验(真实 XOR 校验和、GP/GL、1..4 句、每句 1..4 卫星、句长≤82) |
| `ledger.py` | 持久投递账本(SQLite WAL):待确认周期、片段去重 hash、持久装配状态 |
| `sink.py` | 幂等下游:周期行+确认行同事务提交,确认可验证;故障注入(暂停/丢确认) |
| `collector.py` | 采集器:代际装配、淘汰原因、背压、公平排空、恢复对账;含基线兼容 shim |
| `sim.py` | 确定性源(停等、保留未确认片段、重送)、驱动器、调度令牌、崩溃钩子 |
| `scenarios.py` | 四个命名场景 + 无故障等价校验 + 身份链/字节报告 |
| `experiments.py` | 三个取舍实验(背压、公平性、kill 点矩阵) |
| `cli.py` | 命令行入口 |

## 两个确认边界(工程选择)

**采集确认边界 = wire 片段。** 片段是原子单位:要么完整消费(字节入重组
缓冲、装配状态与产生周期同事务落账本)后返回 ACK,要么整体拒绝
(BACKPRESSURE / NACK_GAP / REJECT)且不消费任何字节。源只保留未确认
片段即可安全重送。关键准则:**已确认片段的内容必须持久** —— 因此行缓冲、
pending 半周期、代内序号计数器都随 `consume_fragment` 同事务持久化
(早期版本只持久化高水位,崩溃丢失易失 pending 导致周期内容丢失,
被 killpoints 实验暴露后修复)。

**下游确认边界 = 周期。** sink 在同一事务写 `cycles` 行与 `acks` 行
(token = sha256(epoch|cycle_id|commit_seq))。"提交后丢确认"只可能丢失
响应消息,确认记录与数据行同生共死。采集端只有 `verify_ack` 通过才回收
账本条目;恢复时对账(`reconcile`)查询下游确认表,幂等键 = 周期身份,
重复投递返回既有确认,恰好保留一行。

**周期身份 = `receiver/g{generation}/c{ordinal}`**,ordinal 是该连接代内
的完成序号(持久化、单调、与账本回收无关)。身份不由卫星内容决定;
内容相同的两个真实周期序号不同,是两个观测。

## 方案选择与放弃原因

1. **账本表示:SQLite(WAL)单事务** —— 放弃 JSONL 追加日志 + 内存索引。
   消费一个片段要原子写入 4 类状态(片段hash/高水位/装配状态/新周期),
   JSONL 需要自行实现行校验和、截断恢复、压实;SQLite 一个事务解决,
   且恢复对账需要按主键随机查询。实验 `killpoints` 验证:12 个命名控制点
   崩溃后全部与无故障一致。
2. **有界账本 + 显式背压** —— 放弃无界队列吸收慢下游。账本满(10)时
   `accept_event` 返回 BACKPRESSURE,不消费、不确认,源保留片段稍后重送;
   内存待确认(flight)≤3。实验 `backpressure` 显示:14 周期输入、下游
   暂停,本方案内存峰值≤3/磁盘峰值=10、零丢失,而无界队列将在采集端
   内存堆积全部 14 个周期。
3. **接收器轮询(RR)排空** —— 放弃全局 FIFO。FIFO 在单接收器突发时让
   其他接收器的已完成周期排队;RR 按接收器轮转。实验 `fairness`:R1 突发
   6 周期 + R2 1 周期积压后,flight=1 排空,R2 周期位次 FIFO=7 / RR=2。
   畸形输入天然隔离:重组与装配按 (receiver,generation) 独立,畸形句只
   淘汰本代 pending 并记录原因,已完成的周期在共享账本中照常排空。

## 运行

```bash
python3 demo.py                                  # 基线正常路径(原样可运行)
python3 cli.py baseline                          # 同上,经 CLI
python3 cli.py run --events fixtures/events.json --schedule fixtures/sink-schedule.json --workdir /tmp/run
python3 cli.py scenario all --workdir /tmp/sc    # 四个确定性演示
python3 cli.py scenario crash-after-complete --workdir /tmp/sc2 --real-kill  # 真实杀进程
python3 cli.py experiment backpressure|fairness|killpoints --workdir /tmp/exp
python3 cli.py verify --events fixtures/events.json --workdir /tmp/vfy
python3 -m pytest tests/ -q                      # 166 个不变量测试
```

调度令牌(fixtures/sink-schedule.json 语义):`pause` 下游暂停 / `receive`、
`commit` 解除暂停允许投递提交 / `lose_ack` 下一次投递提交后丢弃确认 /
`retry` 触发采集端对账回收 / `resume` 清除全部故障模式。

## 真实运行结果(2026-11-15,本仓库 HEAD)

四场景(`scenario all`):slow-consumer 14 周期、backpressure=3、
ledger_hw=10、flight_hw≤3;crash-after-complete 4 周期、恢复重送去重 dup=2;
lost-ack 3 周期、ack_lost 后 retry 对账 recovered=1;reconnect 2 周期
{R1/g2/c0001, R2/g4/c0001},旧代第 1 句与新代第 2 句均被淘汰未拼接。
四者 `equivalent-to-no-fault: True`,`SCENARIOS: PASS`。

killpoints 矩阵:6 个命名控制点 × 2 次命中 = 12 组,全部 PASS。

`pytest`:166 passed。覆盖不变量:跨代隔离(I1)、淘汰原因(I2)、身份稳定
且与内容无关(I3)、恰好一次(I4)、重复片段同字节幂等/冲突拒绝(I5)、
四类界限与显式背压(I6)、畸形隔离(I7)、故障≡无故障(I8)、任意切分/
交织不变(I9)、可验证确认才回收(I10)。

## 仍未覆盖的边界

- 乱序到达的片段:源保证递增,采集端对 gap 显式 NACK,但未实现重排缓冲;
  真乱序网络不在验收边界内。
- 非 ASCII / 超过 82 字符的句:按畸形处理(拒绝或重置本代缓冲),未做
  逐字节纠错。
- 任意磁盘损坏不在边界内;仅覆盖命名控制点杀进程(WAL 已提交事务持久)。
- 接收器上限拒绝后,源侧如何长期调度(换接收器优先级)未建模。
- notices 在内存中无界增长(演示规模无影响;生产应外接日志)。
- 单进程采集;多采集器共用一个下游时的竞争未覆盖(下游幂等键已就绪)。
- 片段预留是保守的(按句数而非实际完成数估计),账本接近满时可能提前
  背压,属安全方向。
