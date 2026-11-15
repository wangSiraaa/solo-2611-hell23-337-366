"""nmea 严格校验:真实校验和、GP/GL 限制、句数/卫星数边界、合法族生成器。"""
import pytest

from nmea import MAX_WIRE, NmeaError, decode, gsv_sentence, sentence


def test_baseline_sentence_roundtrip():
    d = decode(sentence(2, 1, 11))
    assert d == {'talker': 'GP', 'total': 2, 'part': 1,
                 'satellites': ['11', '30', '100', '40']}


def test_checksum_verified_real():
    good = sentence(2, 1, 11)
    body = good[1:].split('*')[0]
    bad = '$' + body + '*00\r\n'
    if bad == good:  # 恰好校验和为 00 时换一个
        bad = '$' + body + '*01\r\n'
    with pytest.raises(NmeaError) as e:
        decode(bad)
    assert e.value.reason == 'checksum'


def test_talker_restriction_gp_gl_only():
    assert decode(gsv_sentence('GL', 1, 1, 1, [5]))['talker'] == 'GL'
    import nmea
    for talker in ('BD', 'GN', 'QZ'):
        body = f'{talker}GSV,1,1,01,05,30,100,40'
        wire = '$' + body + '*' + format(nmea._checksum(body), '02X')
        with pytest.raises(NmeaError) as e:
            decode(wire)
        assert e.value.reason == 'talker'


def test_sentence_type_must_be_gsv():
    wire = '$GPGGA,1,1,02,11,30,100,40*'
    x = 0
    for ch in wire[1:-1]:
        x ^= ord(ch)
    with pytest.raises(NmeaError) as e:
        decode(wire + format(x, '02X'))
    assert e.value.reason == 'sentence_type'


@pytest.mark.parametrize('total,part,reason', [(0, 1, 'total_range'), (5, 1, 'total_range'),
                                               (2, 0, 'part_range'), (2, 3, 'part_range')])
def test_total_part_bounds(total, part, reason):
    s = f'GPGSV,{total},{part},02,11,30,100,40'
    wire = '$' + s + '*' + format(__import__('nmea')._checksum(s), '02X')
    with pytest.raises(NmeaError) as e:
        decode(wire)
    assert e.value.reason == reason


@pytest.mark.parametrize('nsats', [1, 2, 3, 4])
def test_satellite_count_family(nsats):
    d = decode(gsv_sentence('GP', 1, 1, nsats, list(range(1, nsats + 1))))
    assert len(d['satellites']) == 4 * nsats


def test_satellite_count_zero_or_five_rejected():
    s0 = 'GPGSV,1,1,00'
    wire0 = '$' + s0 + '*' + format(__import__('nmea')._checksum(s0), '02X')
    with pytest.raises(NmeaError):
        decode(wire0)
    prns = [1, 2, 3, 4, 5]
    body = 'GPGSV,1,1,05' + ''.join(f',{p},30,100,40' for p in prns)
    wire5 = '$' + body + '*' + format(__import__('nmea')._checksum(body), '02X')
    with pytest.raises(NmeaError) as e:
        decode(wire5)
    assert e.value.reason == 'sat_count'


def test_non_ascii_and_syntax():
    with pytest.raises(NmeaError):
        decode(b'\xff\xfeabc')
    with pytest.raises(NmeaError) as e:
        decode('GPGSV,1,1,02,11,30,100,40*4E')  # 缺 $
    assert e.value.reason == 'syntax'


def test_wire_fragment_limit_constant():
    assert MAX_WIRE == 256
