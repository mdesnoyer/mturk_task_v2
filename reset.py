"""
This function will reset things, bringing them back to a pristine state,
with the exception of preserving the information about the images and image
database.
"""

# remember to kill everything on the port: sudo kill $(sudo lsof -t -i:8080)

from conf import *
from db import Get
from db import Set
import happybase
import logging
import mturk
import os
import boto

reset_database = True  # whether to rebuild the databases
reset_quals = False  # remove qualifications from MTurk
reset_hits = True  # whether or not to remove all extant hits
autopass_practice = True  # whether or not to automatically pass krypton

wid = 'A1RPGGKICRYDKC'  # the krypton worker id

cleanup_log_loc = os.path.join(os.path.expanduser('~'), 'mturk_logs',
                               'reset.log')
logger.config_root_logger(cleanup_log_loc)
_log = logger.setup_logger('reset')
_log.setLevel(logging.DEBUG)


def cleanup_mturk(mtconn):
    """
    Restores mturk sandbox to the pristine state.
    """
    # delete all the qualifications.
    _log.info('Deleting extant qualifications')
    srch_resp = mtconn.search_qualification_types(query=QUALIFICATION_NAME)
    for qual in srch_resp:
        if qual.Name == QUALIFICATION_NAME:
            _log.info('Found "'+ QUALIFICATION_NAME + '"...deleting.')
            mtconn.dispose_qualification_type(qual.QualificationTypeId)
    srch_resp = mtconn.search_qualification_types(query=DAILY_QUOTA_NAME)
    for qual in srch_resp:
        if qual.Name == DAILY_QUOTA_NAME:
            _log.info('Found "' + DAILY_QUOTA_NAME + '"...deleting.')
            mtconn.dispose_qualification_type(qual.QualificationTypeId)
    srch_resp = mtconn.search_qualification_types(query=PRACTICE_QUOTA_NAME)
    for qual in srch_resp:
        if qual.Name == PRACTICE_QUOTA_NAME:
            _log.info('Found "' + PRACTICE_QUOTA_NAME + '"...deleting.')
            mtconn.dispose_qualification_type(qual.QualificationTypeId)


_log.info('Instantiating db connections')
pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)  # make sure to
# instantiate

dbget = Get(pool)
dbset = Set(pool)

_log.info('Intantiating mturk connection')
mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                               aws_secret_access_key=MTURK_SECRET_KEY,
                                               host=MTURK_HOST)

if reset_quals:
    cleanup_mturk(mtconn)

if reset_database:
    dbset.wipe_database_except_images()

_log.info('Instantiating mturk object')
mt = mturk.MTurk(mtconn)

_log.info('Disposing of all HITs')
mt.disable_all_hits_of_type()

if autopass_practice:
    try:
        mt.grant_worker_practice_passed(wid)
    except:
        _log.warn('Could not grant worker passed practice.')
else:
    # revoke it
    try:
        mt.revoke_worker_practice_passed(wid)
    except:
        _log.warn('Could not revoke worker passed practice.')