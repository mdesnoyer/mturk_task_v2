from conf import *
import happybase
import json
import numpy as np

conn = happybase.Connection(host=DATABASE_LOCATION)

table = conn.table(TASK_TABLE)

s = table.scan(columns=['metadata:worker_id'])

times = [json.loads(table.row(x[0]).get('completion_data:response_json'))[
             -1]['time_elapsed'] for x in s]

print 'Mean time is: %.0f min %.0f s' % (divmod(np.mean(times)/1000, 60))
