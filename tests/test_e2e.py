"""端到端:四场景、CLI、真实杀进程、基线 demo 兼容。"""
import json
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLI = os.path.join(REPO, 'cli.py')


def run_cli(*args, cwd=REPO):
    return subprocess.run([sys.executable, CLI] + list(args), capture_output=True,
                          text=True, cwd=cwd)


class TestScenarios:
    @pytest.mark.parametrize('name', ['slow-consumer', 'crash-after-complete',
                                      'lost-ack', 'reconnect'])
    def test_scenario_equivalent_to_no_fault(self, tmp_path, name):
        from scenarios import SCENARIOS
        out = SCENARIOS[name](str(tmp_path / name))
        assert out['equal'], name
        for k, v in out['expect'].items():
            if isinstance(v, bool):
                assert v, f'{name}: expect {k}'

    def test_reconnect_cycle_content(self, tmp_path):
        from scenarios import scenario_reconnect
        out = scenario_reconnect(str(tmp_path))
        cycles = out['fault'].cycles()
        assert set(cycles) == {'R1/g2/c0001', 'R2/g4/c0001'}
        sats = cycles['R1/g2/c0001']['satellites']
        assert sats == ['41', '30', '100', '40', '42', '30', '100', '40']

    def test_identity_chain_intact(self, tmp_path):
        """原句→周期→确认 的链条字段齐备且可验证。"""
        from scenarios import scenario_reconnect
        out = scenario_reconnect(str(tmp_path))
        res = out['fault']
        for _n, cid, payload in res.sink.cycles_with_ids():
            assert cid.startswith(f"{payload['receiver']}/g{payload['generation']}/c")
            assert payload['first_seq'] >= 1
            assert all(s['seq'] >= 1 and len(s['sha1']) == 40
                       for s in payload['sentences'])
            token = res.sink.ack_of(cid)
            assert token and res.sink.verify_ack(cid, token)


class TestCli:
    def test_run_fixture_events(self, tmp_path):
        r = run_cli('run', '--events', os.path.join(REPO, 'fixtures/events.json'),
                    '--workdir', str(tmp_path / 'wd'))
        assert r.returncode == 0, r.stderr
        assert 'R1/g2/c0001' in r.stdout and 'R2/g4/c0001' in r.stdout

    def test_run_with_fixture_schedule(self, tmp_path):
        r = run_cli('run', '--events', os.path.join(REPO, 'fixtures/events.json'),
                    '--schedule', os.path.join(REPO, 'fixtures/sink-schedule.json'),
                    '--workdir', str(tmp_path / 'wd'))
        assert r.returncode == 0, r.stderr

    def test_verify_fixture(self, tmp_path):
        r = run_cli('verify', '--events', os.path.join(REPO, 'fixtures/events.json'),
                    '--workdir', str(tmp_path / 'wd'))
        assert r.returncode == 0 and 'equivalent: True' in r.stdout

    def test_real_kill_and_resume(self, tmp_path):
        """命名控制点真实杀进程(退出码 3),独立进程恢复后集合一致。"""
        from scenarios import ev_crash_after_complete
        ev_path = tmp_path / 'events.json'
        ev_path.write_text(json.dumps(ev_crash_after_complete()))
        wd = str(tmp_path / 'wd')
        base = [sys.executable, CLI, 'run', '--events', str(ev_path), '--workdir', wd]
        r1 = subprocess.run(base + ['--crash-at', 'after_ledger_commit',
                                    '--crash-hit', '2', '--real-kill'],
                            capture_output=True, text=True)
        assert r1.returncode == 3
        r2 = subprocess.run(base + ['--resume'], capture_output=True, text=True)
        assert r2.returncode == 0, r2.stderr
        r3 = subprocess.run([sys.executable, CLI, 'run', '--events', str(ev_path),
                             '--workdir', str(tmp_path / 'clean')],
                            capture_output=True, text=True)
        sig = lambda s: [l for l in s.splitlines() if l.startswith('cycles:')]
        assert sig(r2.stdout) == sig(r3.stdout)

    def test_experiments_pass(self, tmp_path):
        for name, marker in [('backpressure', '丢失=0'), ('fairness', '位次'),
                             ('killpoints', 'KILLPOINTS: PASS')]:
            r = run_cli('experiment', name, '--workdir', str(tmp_path / name))
            assert r.returncode == 0, f'{name}: {r.stdout}{r.stderr}'
            assert marker in r.stdout


class TestBaseline:
    def test_demo_py_unchanged_behavior(self):
        """基线 demo.py 原样运行,正常路径形状保持。"""
        r = subprocess.run([sys.executable, os.path.join(REPO, 'demo.py')],
                           capture_output=True, text=True)
        assert r.returncode == 0, r.stderr
        rows = json.loads(r.stdout)
        assert len(rows) == 1
        row = rows[0]
        assert row['receiver'] == 'R1' and row['talker'] == 'GP'
        assert [p['part'] for p in row['parts']] == [1, 2]
        assert row['parts'][0]['satellites'] == ['11', '30', '100', '40']

    def test_legacy_collector_accepts_baseline_calls(self):
        from collector import Collector
        c = Collector()
        assert c.accept('R1', 1, __import__('nmea').sentence(2, 1, 11)) is None
        out = c.accept('R1', 1, __import__('nmea').sentence(2, 2, 12))
        assert out['talker'] == 'GP' and len(out['parts']) == 2
        assert len(c.output) == 1
