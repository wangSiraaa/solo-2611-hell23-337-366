def allowed(c,user):return [r[0] for r in c.execute('SELECT tenant FROM grant_data WHERE user=?',(user,))]
