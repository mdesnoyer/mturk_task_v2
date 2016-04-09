"""
Manually unban workers
"""

from db import Get
from db import Set
from mturk import MTurk
import boto.mturk.connection
import happybase
from conf import *
import boto.ses

pool = happybase.ConnectionPool(size=8, host=DATABASE_LOCATION)
dbget = Get(pool)
dbset = Set(pool)

mtconn = boto.mturk.connection.MTurkConnection(
            aws_access_key_id=MTURK_ACCESS_ID,
            aws_secret_access_key=MTURK_SECRET_KEY,
            host=MTURK_HOST)

mt = MTurk(mtconn)
mt.setup_quals()

emconn = boto.ses.connect_to_region('us-east-1')

def dispatch_notification(message, subject='Notification'):
    """
    Dispatches a message to kryptonlabs99@gmail.com

    :param message: The body of the message.
    :param subject: The subject of the message.
    """
    emconn.send_email('ops@kryto.me', subject, message,
                      ['kryptonlabs99@gmail.com'])


print 'Checking if any bans can be lifted...'
workers = dbget.get_all_workers()
banned_workers = []
for worker_id in workers:
    if dbget.worker_is_banned(worker_id):
        banned_workers.append(worker_id)
        if not dbset.worker_ban_expires_in(worker_id):
            mt.unban_worker(worker_id)
            dispatch_notification('Worker %s has been unbanned' % str(
                worker_id), subject="Unban notification")

