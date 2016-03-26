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

s = "Updates on choosing images"
x = ["Hello everyone-",]
x += ["Thank you all for the numerous messages of support " \
    "and advice. While we work out a more permanent solution, we will be " \
    "posting more HITs so you can perform them over the weekend. However, " \
    "importantly, no new workers will be allowed to work on them--only those " \
    "of you who have already submitted at least one non-practice HIT. One of " \
    "the things that has made these HITs so successful has been the " \
    "word-of-mouth from all of your, helping to spread information via a " \
    "number of forums. Unfortunately, no one new will be able to take these " \
    "HITs, for the time being."]
x += ["New HITs will be going online shortly."]
x += ["Additionally, when a user is issued a block " \
    "qualification, their practice qualification is revoked. Since no new " \
    "practices will be posted, these blocks have become, in effect, " \
    "permanent. Thus you may contact us directly to appeal any blocks as you " \
    "have recieved."]
x += ["Because this will mean modifying our code, please excuse any bugs that"
      "may occur and contact us immediately if you notice any. Thank you "
      "again for participating in our experiment and for your continued "
      "support."]
print '\n\n'.join(x)

err_workers = []
for i in range((len(workers)/c_size)+1):
    j = workers[(i*c_size):((i+1)*c_size)]
    try:
        mtconn.notify_workers(j, s, x)
    except:
        err_workers += i
