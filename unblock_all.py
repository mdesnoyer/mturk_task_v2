from conf import *
from db import Get
from db import Set
import happybase
import logging
import mturk
import os
import boto

pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)

dbget = Get(pool)

mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                               aws_secret_access_key=MTURK_SECRET_KEY,
                                               host=MTURK_HOST)

workers = [w for w in dbget.get_all_workers()]
banned_ids = filter(lambda x: dbget.worker_is_banned(x), workers)

for b in banned_ids:
    mtconn.unblock_worker(b, 'We will no longer be blocking people, apologies for an inconvenience this has caused!')