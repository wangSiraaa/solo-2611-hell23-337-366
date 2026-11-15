from database import connect
def remaining(path):
 c=connect(path);r={t:c.execute('SELECT * FROM '+t).fetchall() for t in ['account','event','legal_hold']};c.close();return r
