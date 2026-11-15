"""NMEA-0183 GSV 句解析与校验。

decode()/sentence() 保持基线签名;decode 现为严格校验:
仅接受 GP/GL talker 的 GSV 句,校验真实 XOR 校验和、句长、
字段数量与取值范围(每周期 1..4 句、每句 1..4 卫星)。
"""

ALLOWED_TALKERS = ('GP', 'GL')
MAX_TOTAL = 4          # 每周期最多句数
MAX_SATS = 4           # 每句最多卫星数
MAX_LINE = 82          # NMEA-0183 句长上限(含 CRLF)
MAX_WIRE = 256         # wire 片段上限(字节)


class NmeaError(ValueError):
    """带机器可读 reason 的解析/校验错误。"""

    def __init__(self, reason, detail=''):
        super().__init__(f'{reason}: {detail}' if detail else reason)
        self.reason = reason


def _checksum(body):
    x = 0
    for ch in body:
        x ^= ord(ch)
    return x


def decode(line):
    """解析并严格校验一条 GSV 句(可带 CRLF)。

    返回 {'talker','total','part','satellites'}(与基线同形)。
    校验失败抛 NmeaError,reason 取值:
    non_ascii/too_long/syntax/checksum/sentence_type/talker/
    field_numeric/total_range/part_range/sat_fields/sat_count/sat_range
    """
    if isinstance(line, bytes):
        try:
            line = line.decode('ascii')
        except UnicodeDecodeError:
            raise NmeaError('non_ascii')
    raw = line.strip()
    if len(raw) + 2 > MAX_LINE:
        raise NmeaError('too_long', f'{len(raw) + 2} > {MAX_LINE}')
    if not raw.startswith('$') or '*' not in raw:
        raise NmeaError('syntax', raw[:16])
    body, _, check = raw[1:].rpartition('*')
    if len(check) != 2:
        raise NmeaError('checksum', check)
    try:
        want = int(check, 16)
    except ValueError:
        raise NmeaError('checksum', check)
    if _checksum(body) != want:
        raise NmeaError('checksum', f'{_checksum(body):02X} != {check.upper()}')
    a = body.split(',')
    if len(a) < 4:
        raise NmeaError('sat_fields', body)
    sid = a[0]
    if len(sid) != 5 or sid[2:] != 'GSV':
        raise NmeaError('sentence_type', sid)
    talker = sid[:2]
    if talker not in ALLOWED_TALKERS:
        raise NmeaError('talker', talker)
    try:
        total = int(a[1])
        part = int(a[2])
        int(a[3])  # in-view 数目,仅要求为数字(与每句卫星数无强制关系)
    except ValueError:
        raise NmeaError('field_numeric', body)
    if not 1 <= total <= MAX_TOTAL:
        raise NmeaError('total_range', str(total))
    if not 1 <= part <= total:
        raise NmeaError('part_range', f'{part}/{total}')
    rest = a[4:]
    if len(rest) == 0 or len(rest) % 4 != 0:
        raise NmeaError('sat_fields', f'{len(rest)} fields')
    k = len(rest) // 4
    if not 1 <= k <= MAX_SATS:
        raise NmeaError('sat_count', str(k))
    for i in range(k):
        prn, el, az, snr = rest[4 * i:4 * i + 4]
        try:
            prn, el, az, snr = int(prn), int(el), int(az), int(snr)
        except ValueError:
            raise NmeaError('sat_fields', rest[4 * i:4 * i + 4].__str__())
        if not (1 <= prn and 0 <= el <= 90 and 0 <= az <= 359 and 0 <= snr <= 99):
            raise NmeaError('sat_range', f'{prn},{el},{az},{snr}')
    return {'talker': talker, 'total': total, 'part': part, 'satellites': rest}


def sentence(total, part, prn):
    """基线生成器:单卫星 GPGSV 句(fixtures 依赖,逐字节保持)。"""
    s = f'GPGSV,{total},{part},02,{prn},30,100,40'
    return '$' + s + '*' + format(_checksum(s), '02X') + '\r\n'


def gsv_sentence(talker, total, part, in_view, prns):
    """合法输入族生成器:talker ∈ GP/GL,1..4 句,每句 1..4 卫星。"""
    assert talker in ALLOWED_TALKERS and 1 <= total <= MAX_TOTAL
    assert 1 <= part <= total and 1 <= len(prns) <= MAX_SATS
    body = f'{talker}GSV,{total},{part},{in_view:02d}'
    for p in prns:
        body += f',{p},30,100,40'
    return '$' + body + '*' + format(_checksum(body), '02X') + '\r\n'
