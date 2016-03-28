"""
Takes the win data and dumps it to a CSV file in /tmp as /tmp/win_data.csv
"""

from conf import *
import happybase

conn = happybase.Connection(DATABASE_LOCATION)

t = conn.table(WIN_TABLE)
s = t.scan()

with open('/tmp/data', 'w') as f:
    for n, (k, data) in enumerate(s):
        if not n % 1000:
            print n
        winner = data['data:winner_id']
        loser = data['data:loser_id']
        win_cnt = t.counter_get(k, 'data:win_count')
        f.write('%s,%s,%i\n' % (winner, loser, win_cnt))