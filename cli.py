"""命令行入口。

  python cli.py baseline                     基线正常路径(等价 python demo.py)
  python cli.py run --events E --workdir D [--schedule S] [--resume]
                    [--crash-at P --crash-hit N --real-kill] [--drain rr|fifo]
  python cli.py scenario NAME --workdir D    确定性演示(slow-consumer /
                    crash-after-complete / lost-ack / reconnect / all)
  python cli.py experiment NAME --workdir D  取舍实验(backpressure /
                    fairness / killpoints)
  python cli.py verify --events E --workdir D [--schedule S]
"""
import argparse
import json
import os
import subprocess
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from scenarios import SCENARIOS, fresh_dir, run_case  # noqa: E402
from sim import load_events, load_schedule  # noqa: E402


def _print_result(res, title):
    print(f'--- {title} ---')
    for line in res.chain_lines():
        print(line)
    rep = res.byte_report()
    st = rep['stats']
    print(f"stats: accepted={st['accepted']} dup={st['duplicates']} "
          f"backpressure={st['backpressure']} evicted={st['evicted']} "
          f"malformed={st['malformed']} delivered={st['delivered']} "
          f"recovered={st['recovered']} ledger_hw={st['ledger_hw']} flight_hw={st['flight_hw']}")
    print(f"bytes: ledger_pending_payload={rep['ledger_payload_bytes_pending']} "
          f"ledger_file={rep['ledger_file_bytes']} sink_file={rep['sink_file_bytes']}")
    print(f"cycles: {len(res.cycles())}  signature={res.signature()}")


def cmd_baseline(_args):
    import runpy
    runpy.run_path(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'demo.py'),
                   run_name='__main__')


def cmd_run(args):
    from sim import SimulatedCrash
    events = load_events(args.events)
    schedule = load_schedule(args.schedule) if args.schedule else ()
    try:
        res = run_case(events, args.workdir, schedule=schedule, resume=args.resume,
                       crash_at=args.crash_at, crash_hit=args.crash_hit,
                       drain_policy=args.drain, real_kill=args.real_kill)
    except SimulatedCrash as e:
        print(f'simulated crash at control point: {e} (exit 3, 用 --resume 恢复)')
        return 3
    _print_result(res, f'run {args.workdir}')
    for n in res.collector.notices:
        if n.kind in ('evicted', 'malformed', 'backpressure', 'reject', 'ack_lost',
                      'recovered', 'sink_paused'):
            print(f'  notice: {n.kind} {n.receiver}/g{n.generation} {n.reason} {n.detail}')
    return 0


def cmd_scenario(args):
    names = list(SCENARIOS) if args.name == 'all' else [args.name]
    ok = True
    for name in names:
        if name == 'crash-after-complete' and args.real_kill:
            ok &= _scenario_crash_real(args.workdir)
            continue
        out = SCENARIOS[name](os.path.join(args.workdir, name))
        print(f'=== scenario {name} ===')
        print(f"expect: {out['expect']}")
        _print_result(out['fault'], 'fault run')
        print(f"equivalent-to-no-fault: {out['equal']}")
        ok &= bool(out['equal'])
    print('SCENARIOS:', 'PASS' if ok else 'FAIL')
    return 0 if ok else 1


def _scenario_crash_real(workdir):
    """真实杀进程演示:子进程在控制点 os._exit,再独立进程恢复。"""
    from scenarios import ev_crash_after_complete
    d = fresh_dir(os.path.join(workdir, 'crash-after-complete'))
    ev_path = os.path.join(d, 'events.json')
    with open(ev_path, 'w') as f:
        json.dump(ev_crash_after_complete(), f)
    env = dict(os.environ)
    base = [sys.executable, os.path.abspath(__file__), 'run', '--events', ev_path,
            '--workdir', os.path.join(d, 'fault')]
    r1 = subprocess.run(base + ['--crash-at', 'after_ledger_commit', '--crash-hit', '2',
                                '--real-kill'], capture_output=True, text=True, env=env)
    print(f'=== scenario crash-after-complete (real kill) ===')
    print(f'phase1 exit code: {r1.returncode} (期望 3 = 控制点杀进程)')
    if r1.returncode != 3:
        print(r1.stdout, r1.stderr)
        return False
    r2 = subprocess.run(base + ['--resume'], capture_output=True, text=True, env=env)
    print(r2.stdout)
    r3 = subprocess.run([sys.executable, os.path.abspath(__file__), 'run',
                         '--events', ev_path, '--workdir', os.path.join(d, 'clean')],
                        capture_output=True, text=True, env=env)
    sig = lambda out: [l for l in out.splitlines() if l.startswith('cycles:')]
    equal = sig(r2.stdout) == sig(r3.stdout)
    print(f'equivalent-to-no-fault: {equal}')
    return r2.returncode == 0 and equal


def cmd_experiment(args):
    from experiments import exp_backpressure, exp_fairness, exp_killpoints
    fresh_dir(args.workdir)
    if args.name == 'backpressure':
        r = exp_backpressure(os.path.join(args.workdir, 'bp'))
        print('=== experiment: 有界账本+背压 vs 无界队列 ===')
        print(f"交付周期={r['delivered']} 丢失={r['lost']} 背压事件={r['backpressure_events']}")
        print(f"本方案: 内存待确认峰值={r['bounded_memory_cycles']} (<=3), "
              f"磁盘账本峰值={r['bounded_disk_cycles']} (<=10)")
        print(f"无界队列对照: 内存将堆积={r['unbounded_memory_cycles']} 个周期")
        print(f"背压期间由源保留的未确认片段={r['source_retained']} (采集端零堆积)")
        return 0 if r['lost'] == 0 and r['delivered'] == 14 else 1
    if args.name == 'fairness':
        r = exp_fairness(os.path.join(args.workdir, 'fair'))
        print('=== experiment: RR 公平排空 vs 全局 FIFO ===')
        for policy in ('fifo', 'rr'):
            print(f"{policy}: R2 周期排空位次={r[policy]['r2_drain_position']}  "
                  f"顺序={r[policy]['drain_order']}")
        return 0
    if args.name == 'killpoints':
        r = exp_killpoints(os.path.join(args.workdir, 'kill'))
        print('=== experiment: 命名控制点崩溃矩阵(SQLite 单事务账本) ===')
        ok = True
        for k, v in sorted(r.items()):
            print(f'  {k:38s} {v}')
            ok &= v == 'PASS'
        print('KILLPOINTS:', 'PASS' if ok else 'FAIL')
        return 0 if ok else 1
    raise SystemExit(f'unknown experiment {args.name}')


def cmd_verify(args):
    from scenarios import run_with_faults
    events = load_events(args.events)
    schedule = load_schedule(args.schedule) if args.schedule else ()
    fault, clean, equal = run_with_faults(events, args.workdir, schedule=schedule)
    print(f'fault-run cycles: {sorted(fault.cycles())}')
    print(f'clean-run cycles: {sorted(clean.cycles())}')
    print(f'equivalent: {equal}')
    return 0 if equal else 1


def main(argv=None):
    p = argparse.ArgumentParser(prog='gsvcol', description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest='cmd', required=True)
    sub.add_parser('baseline').set_defaults(f=cmd_baseline)

    pr = sub.add_parser('run')
    pr.add_argument('--events', required=True)
    pr.add_argument('--schedule')
    pr.add_argument('--workdir', required=True)
    pr.add_argument('--resume', action='store_true')
    pr.add_argument('--crash-at')
    pr.add_argument('--crash-hit', type=int, default=1)
    pr.add_argument('--real-kill', action='store_true')
    pr.add_argument('--drain', choices=('rr', 'fifo'), default='rr')
    pr.set_defaults(f=cmd_run)

    ps = sub.add_parser('scenario')
    ps.add_argument('name', choices=list(SCENARIOS) + ['all'])
    ps.add_argument('--workdir', required=True)
    ps.add_argument('--real-kill', action='store_true')
    ps.set_defaults(f=cmd_scenario)

    pe = sub.add_parser('experiment')
    pe.add_argument('name', choices=('backpressure', 'fairness', 'killpoints'))
    pe.add_argument('--workdir', required=True)
    pe.set_defaults(f=cmd_experiment)

    pv = sub.add_parser('verify')
    pv.add_argument('--events', required=True)
    pv.add_argument('--schedule')
    pv.add_argument('--workdir', required=True)
    pv.set_defaults(f=cmd_verify)

    args = p.parse_args(argv)
    return args.f(args)


if __name__ == '__main__':
    sys.exit(main())
