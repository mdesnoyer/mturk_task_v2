# we have to make sure all the quotas are being updated properly.
# urg!
# To obtain dbset / dbget
from conf import *
from db import Get
from db import Set
import happybase
import logging
import mturk
import os
import boto

pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)

dbset = Set(pool)
dbget = Get(pool)

mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                               aws_secret_access_key=MTURK_SECRET_KEY,
                                               host=MTURK_HOST)
mt = mturk.MTurk(mtconn)
mt.setup_quals()

for worker_id in [x for x in dbget.get_all_workers()]:
    print 'setting %s' % worker_id
    mt.reset_worker_daily_quota(worker_id)

for worker_id in [x for x in dbget.get_all_workers()]:
    print 'verifying %s' % worker_id
    qscore = mt.get_qualification_score(mt.quota_id,worker_id)
    if qscore != MAX_SUBMITS_PER_DAY:
        print 'Bad qual score for %s, value %i' % (worker_id, qscore)