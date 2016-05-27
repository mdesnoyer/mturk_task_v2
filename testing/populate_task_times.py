"""
This is a utility function that will simply create a field
within the completion data that includes the total time in seconds.

At least, that was its original intent. However, it looks like the original
get_mean_times function was extremely inefficient. so I updated it, and this
might be sufficient on its own.

ITS NOT! Not by like a massive longshot...

so, back to the original plan of using the distinct column.
"""

from conf import *
import happybase
import json

_log = logger.setup_logger(__name__)

conn = happybase.Connection(host=DATABASE_LOCATION)
dst_table = conn.table(TASK_TABLE)
src_table = conn.table(TASK_JSON_TABLE)

num = 0
print 'Counting rows'
s = dst_table.scan(filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
for n, (id, _) in enumerate(s):
    if not n % 10:
        print n
    num += 1

s = src_table.scan(columns=['completion_data:response_json'],
                   batch_size=10)
for n, (id, data) in enumerate(s):
    if not n % 10:
        print '%i / %i' % (n, num)
    jsn_str = data.get('data:json', None)
    if jsn_str is None:
        continue
    jsn = json.loads(jsn_str)
    tot_time = jsn[-1]['time_elapsed']
    dst_table.put(id, {'completion_data:total_time': str(tot_time)})

