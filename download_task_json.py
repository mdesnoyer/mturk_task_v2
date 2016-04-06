"""
Batch download of task JSON data, for validation purposes.

DATABASE: 10.0.36.202
WEBSERVER: 10.0.35.248
ssh -i ~/.ssh/mturk_stack_access.pem ubuntu@10.0.35.248

tunnel to the database:
ssh -f -i ~/.ssh/mturk_stack_access.pem ubuntu@10.0.36.202 -L 9090:10.0.36.202:9090 -N
"""
from conf import *
import os
import happybase
import dill
import numpy as np

# pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)

dest = '/data/mturk_task_json'
# dbget = Get(pool)
conn = happybase.Connection('localhost')
print 'Connection established'
t = conn.table(TASK_TABLE)

print 'fetching accepted tasks'
acc_tasks = [tid for tid, acc in t.scan(columns=['status:accepted']) if acc['status:accepted'] == TRUE]

print 'starting task fetch'

# step_size = 100
# for n in np.arange(0, len(acc_tasks), step_size):
#     print '%i / %i' % (n + step_size, len(acc_tasks))
#     r = t.rows(acc_tasks[n:(n+step_size)])
#     break

for n, acc_id in enumerate(acc_tasks):
    print '%i / %i' % (n + 1, len(acc_tasks))
    nfn = os.path.join(dest, acc_id)
    if os.path.exists(nfn):
        continue
    r = t.row(acc_id)
    with open(nfn, 'w') as f:
        dill.dump(r, f)