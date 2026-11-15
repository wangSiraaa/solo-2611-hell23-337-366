import sqlite3,json
class Sink:
    def __init__(self,path):
        self.c=sqlite3.connect(path);self.c.execute('CREATE TABLE IF NOT EXISTS cycles(n INTEGER PRIMARY KEY, payload TEXT)');self.c.commit()
    def send(self,cycle):self.c.execute('INSERT INTO cycles(payload) VALUES(?)',(json.dumps(cycle),));self.c.commit()
    def rows(self):return [json.loads(x[0]) for x in self.c.execute('SELECT payload FROM cycles ORDER BY n')]
