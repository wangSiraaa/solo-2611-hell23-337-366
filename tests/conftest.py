import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from nmea import gsv_sentence
from scenarios import Numberer, cycle_events, data, disc


@pytest.fixture()
def numberer():
    return Numberer()


def make_cycle_events(rx, gen, total, prns_per_part, talker='GP'):
    return cycle_events(rx, gen, total, prns_per_part, talker)


def split_every_offset(wire):
    """对一条 wire,产出所有单点切分方案的 [prefix, rest]。"""
    return [[wire[:i], wire[i:]] for i in range(1, len(wire))]


def fragment_with(splitter):
    """splitter(wire)->[pieces],返回事件变换函数(重编 seq)。"""
    from sim import fragment_events
    return lambda events: fragment_events(events, splitter)
