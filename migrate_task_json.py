'''
Migrates the task JSON, from the task table to the task
json table.
'''

from conf import *
import happybase

conn = happybase.Connection(host=DATABASE_LOCATION)
src_table = conn.table(TASK_TABLE)
dst_table = conn.table(TASK_JSON_TABLE)

s = src_table.scan(columns=['completion_data:response_json'],
                   batch_size=10)

for n, (id, data) in enumerate(s):
    if not n % 10:
        print '%i' % n
    jsn_str = data.get('completion_data:response_json', None)
    if jsn_str is None:
        continue
    dst_table.put(id, {'data:json': jsn_str})
