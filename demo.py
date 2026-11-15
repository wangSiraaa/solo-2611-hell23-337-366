import json,sqlite3
from executor import init,execute
from planner import plan
c=sqlite3.connect(':memory:');init(c);p=plan(c,[{'key':'A','delta':5}]);execute(c,p);print(json.dumps({'plan':p,'rows':c.execute('SELECT * FROM target').fetchall()}))
