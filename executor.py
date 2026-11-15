def execute(c,actions):
    with c:
        for a in actions:
            if a['kind']=='update':c.execute('UPDATE target SET amount=?,rev=rev+1 WHERE id=?',(a['amount'],a['id']))
            else:c.execute('INSERT INTO target(business_key,amount,rev) VALUES(?,?,1)',(a['key'],a['amount']))
def init(c):c.executescript('CREATE TABLE target(id INTEGER PRIMARY KEY,business_key TEXT UNIQUE,amount INTEGER,rev INTEGER); INSERT INTO target VALUES(1,"A",10,1),(2,"B",20,1);')
