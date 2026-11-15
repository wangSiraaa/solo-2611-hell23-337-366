def decode(line):
    body,check=line.strip()[1:].split('*');x=0
    for ch in body:x^=ord(ch)
    if x!=int(check,16):raise ValueError('checksum')
    a=body.split(',');return {'talker':a[0][:2],'total':int(a[1]),'part':int(a[2]),'satellites':a[4:]}
def sentence(total,part,prn):
    s=f'GPGSV,{total},{part},02,{prn},30,100,40';x=0
    for ch in s:x^=ord(ch)
    return '$'+s+'*'+format(x,'02X')+'\r\n'
