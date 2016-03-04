"""
This function will reset things, bringing them back to a pristine state,
with the exception of preserving the information about the images and image
database.
"""

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
autopass_practice = False  # whether or not to automatically pass krypton

wid = 'A1RPGGKICRYDKC'  # the krypton worker id

file_root = os.path.dirname(os.path.realpath(__file__))
logger.config_root_logger(os.path.join(file_root, 'logs/scratch_setup.log'))
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
conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate

dbget = Get(conn)
dbset = Set(conn)

_log.info('Intantiating mturk connection')
mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                               aws_secret_access_key=MTURK_SECRET_KEY,
                                               host='mechanicalturk.sandbox.amazonaws.com')

if reset_quals:
    cleanup_mturk(mtconn)

if reset_database:
    dbset.wipe_database_except_images()

_log.info('Instantiating mturk object')
mt = mturk.MTurk(mtconn)

_log.info('Disposing of all HITs')
mt.disable_all_hits_of_type()

if autopass_practice:
    mt.grant_worker_practice_passed(wid)