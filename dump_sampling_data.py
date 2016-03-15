"""
Dumps the image sampling data to a file (/tmp/datas) as a stored numpy array.
"""

from conf import *
from db import Get
import happybase
import numpy as np

conn = happybase.Connection(DATABASE_LOCATION)
pool = happybase.ConnectionPool(size=3, host=DATABASE_LOCATION)
dbget = Get(pool)

print 'Getting active images'
active_ids = []
samp_cnts = []
win_cnts = []
samp_surp = []
t = conn.table(IMAGE_TABLE)

scnr = dbget.get_active_image_scanner()
for n in range(dbget.get_n_active_images_count()):
    x = scnr.next()
    active_ids.append(x)
    samp_cnts.append(t.counter_get(x, 'stats:num_times_seen'))
    win_cnts.append(t.counter_get(x, 'stats:num_wins'))
    samp_surp.append(t.counter_get(x, 'stats:sampling_surplus'))
    print n

with open('/tmp/datas', 'w') as f:
    np.save(f, np.array([active_ids, samp_cnts, win_cnts, samp_surp]))


