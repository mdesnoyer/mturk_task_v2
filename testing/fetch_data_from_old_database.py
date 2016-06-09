# script for fetching all the info from the server

from __future__ import generators
import numpy as np
import psycopg2 as dbapi
from collections import defaultdict as ddict


host = 'ec2-54-197-241-67.compute-1.amazonaws.com'
database = 'demek58s78bbkn'
port = 5432
password = 'WHLuEoq7zS26xZPutJlLQA487Y'
user = 'zogqtvlfduwuiy'

def ResultIter(cursor, arraysize=1000):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

print 'Making connection'
db_connection = dbapi.connect(database=database, host=host, user=user,
                              password=password, port=port)
c = db_connection.cursor()

print 'Getting tables'
c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
tables = c.fetchall()

for (i, ) in tables:
    print i

table = 'image_choices'
print 'Getting columns - table'
exstr = "SELECT * FROM %s LIMIT 2" % table
c.execute(exstr)
choice_columns = [x.name for x in c.description]

table = 'worker_info'
print 'Getting columns - workers'
exstr = "SELECT * FROM %s LIMIT 2" % table
c.execute(exstr)
worker_columns = [x.name for x in c.description]

print 'Getting worker info'
exstr = "SELECT (id, worker_id, gender, age_group) FROM worker_info"
c.execute(exstr)
d = c.fetchall()
d = np.array([x[0].replace('(','').replace(')','').split(',') for x in d])
wdata = ddict(lambda: [None, None])
for idx, wid, gender, age_grp in d:
    if not wid:
        continue
    cgender = None
    if gender == 'M':
        cgender = 'male'
    elif gender == 'F':
        cgender = 'female'
    wdata[wid] = [cgender, age_grp]

keep_key = 'KEEP'
rej_key = 'RETURN'

table = 'image_choices'
exstr = "SELECT * FROM %s" % table
with db_connection.cursor('server_side_cursor') as cd:
    cd.execute(exstr)
    cd.scroll(0)
    curiter = ResultIter(cd)
    with open('/tmp/old_data','w') as f:
        for n, (cid, aid, i1, i2, i3, choice, _, _, con, _, wid, _, _) in enumerate(curiter):
            if not n % 100:
                print n, cid, cd.rownumber
            gender, age = wdata[wid]
            if con == keep_key:
                tups = [(choice, x) for x in [i1, i2, i3] if x != choice]
            elif con == rej_key:
                tups = [(x, choice) for x in [i1, i2, i3] if x != choice]
            for i, j in tups:
                f.write('%s,%s,%s,%s,%s\n' % (i, j, gender, age, wid))
