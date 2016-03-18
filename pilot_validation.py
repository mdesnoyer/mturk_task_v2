# This script is designed to validate the data obtained from Pilot IV
from conf import *
import happybase
import logging
import os
from collections import defaultdict as ddict
from collections import Counter
import numpy as np

val_log_loc = \
    os.path.join(os.path.expanduser('~'), 'mturk_logs', 'pilot_validation.log')
logger.config_root_logger(val_log_loc)
_log = logger.setup_logger('val')
_log.setLevel(logging.DEBUG)

_log.info('Instantiating db connections')
conn = happybase.Connection(host=DATABASE_LOCATION)

# fetch all the data
table = conn.table(WIN_TABLE)
s = table.scan()

obs_imgs = set()
win_dict = ddict(lambda: Counter())
win_cnts = Counter()
for n, (uid, data) in enumerate(s):
    print n, len(obs_imgs)
    win = data.get('data:winner_id')
    los = data.get('data:loser_id')
    obs_imgs.add(win)
    obs_imgs.add(los)
    wc =  table.counter_get(uid, 'data:win_count')
    win_dict[win][los] += wc
    win_cnts[win] += wc

# shockingly this all worked
