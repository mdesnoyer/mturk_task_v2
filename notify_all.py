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

workers = [w for w in dbget.get_all_workers()]

c_size = 20

subj = "Updates on choosing images"
letter = 'letter_to_participants'
text = open(letter, 'r').read()

err_workers = []
for i in range((len(workers)/c_size)+1):
    j = workers[(i*c_size):((i+1)*c_size)]
    try:
        mtconn.notify_workers(j, subj, text)
    except:
        err_workers += i
