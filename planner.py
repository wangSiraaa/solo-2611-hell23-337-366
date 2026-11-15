def plan(connection,source):
    actions=[]
    for row in source:
        old=connection.execute('SELECT id,business_key,amount,rev FROM target WHERE business_key=?',(row['key'],)).fetchone()
        if old:actions.append({'kind':'update','id':old[0],'amount':old[2]+row['delta'],'observed_rev':old[3]})
        else:actions.append({'kind':'insert','key':row['key'],'amount':row['delta']})
    return actions
