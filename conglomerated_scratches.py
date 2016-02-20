"""
This is the first scratch file that begins the process of end-to-end testing for the MTurk app. Each one of these is
a standalone module, because the imports may change idfk i friggin hate pyramid.
"""

"""
TEST 1: TASK GENERATION
    Subtest: Adding images
    Subtest: Activating images

Should be performable from within the mturk_task_v2 directory.

BEFORE RUNNING:
    - Make sure to start the hbase stuff, by running
        /hbase/hbase-1.1.2/bin/start-hbase.sh
        hbase thrift start
"""
from conf import *
from db import Get
from db import Set
import happybase
from glob import glob
import logging
import mturk
import os
import boto
import ipdb

# # testing params
# n_images = 300
# to_activate = 100
#
# _log = logger.setup_logger('back to front test I')
# _log.setLevel(logging.DEBUG)
#
# _log.info('Connecting to database')
# conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate the database connection.
#
# _log.info('Instantiating db connections')
# dbget = Get(conn)
# dbset = Set(conn)
#
# _log.info('Rebuilding tables')
# dbset.force_regen_tables()
#
# images = glob('static/images/*.jpeg')
# _log.info('Found %i images' % len(images))
# _log.info('Selecting %i to be input into the database' % n_images)
#
# ims = list(np.random.choice(images, n_images, replace=False))
#
# im_ids = [x.split('/')[-1].split('.')[0] for x in ims] # get the image ids
# _log.info('Registering images...')
# dbset.register_images(im_ids[:295], ims[:295])
# dbset.register_images(im_ids[295:], ims[295:], attributes=['test'])
#
# _log.info('Activating first 200 images...')
# dbset.activate_images(im_ids[:200])

"""
TEST 2: TASK GENERATION
    Subtest: Generate a HIT type.
    Subtest: Register a HIT type.
    Subtest: Get a HIT type.
    Subtest: Generate a task.
    Subtest: Register a task.
"""
from conf import *
from db import Get
from db import Set
import happybase
from glob import glob
import logging
import mturk
import os
import boto
import ipdb
from time import sleep

_log = logger.setup_logger('back to front test I')
_log.setLevel(logging.DEBUG)

_log.info('Connecting to database')
conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate the database connection.

_log.info('Instantiating db connections')
dbget = Get(conn)
dbset = Set(conn)

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
    # it appears that the incorrect qualification ids will be fetched if they're fetched too soon after being deleted.
    # to avoid this, we sleep for some amount of 60 sec after they've been deleted.

MTURK_ACCESS_ID = os.environ['MTURK_ACCESS_ID']
MTURK_SECRET_KEY = os.environ['MTURK_SECRET_KEY']


_log.info('Intantiating mturk connection')
mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                               aws_secret_access_key=MTURK_SECRET_KEY,
                                               host='mechanicalturk.sandbox.amazonaws.com')

my_worker_id = 'A1RPGGKICRYDKC'
mt = mturk.MTurk(mtconn)

# # we don't need to cleanup mturk anymore, i'm very confident in the effectiveness of the create / identify
# # qualifications functionality of the MTurk class
# while True:
#     try:
#         cleanup_mturk(mtconn)
#         break
#     except:
#         _log.info('Qualification deletion error, please wait.')
#         sleep(10)
#         _log.info('Trying again...')

# testing params
n_images = 300
to_activate = 100

_log.info('Rebuilding tables')
dbset.force_regen_tables()

images = glob('static/images/*.jpeg')
_log.info('Found %i images' % len(images))
_log.info('Selecting %i to be input into the database' % n_images)

ims = list(np.random.choice(images, n_images, replace=False))

im_ids = [x.split('/')[-1].split('.')[0] for x in ims] # get the image ids
_log.info('Registering images...')
dbset.register_images(im_ids[:295], ims[:295])
dbset.register_images(im_ids[295:], ims[295:], attributes=['test'])

_log.info('Activating first 200 images...')
dbset.activate_images(im_ids[:200])

_log.info('Instantiating mturk object')
while True:
    try:
        mt = mturk.MTurk(mtconn)
        break
    except:
        _log.info('Qualifications still exist, please wait.')
        sleep(10)
        _log.info('Trying again...')

_log.info('Disposing of all HITs')
mt.disable_all_hits_of_type()

_log.info('Disposing of all HIT types')
mt.dispose_of_hit_type()

# _log.info('Attempting to generate a HIT type')
# tid, prac_tid = mt.register_hit_type_mturk()  # no arguments are needed at this point.
#
# _log.info('Registering hit type -- standard task')
# dbset.register_hit_type(tid, active=True)
#
# _log.info('Registering hit type -- practice task')
# dbset.register_hit_type(prac_tid, is_practice=True, active=True)
#
# _log.info('Attempting to generate a task')
# task_id, exp_seq, attribute, register_task_kwargs = dbget.gen_task(DEF_PRACTICE_NUM_IMAGES_PER_TASK, 3,
#                                                                    DEF_NUM_IMAGE_APPEARANCE_PER_TASK,
#                                                                    n_keep_blocks=1,
#                                                                    n_reject_blocks=1,
#                                                                    practice=True,
#                                                                    hit_type_id=prac_tid) # SUCCESS!
#
#
# _log.info('Attempting to register the obtained task')
# dbset.register_task(task_id, exp_seq, attribute, **register_task_kwargs)  # WORKS TOO!
#
# import ipdb
# ipdb.set_trace()
#
# _log.info('Attempting to fetch the Qualification object')
# qual = mtconn.get_qualification_type(mt.qualification_id)[0]
# """
# TEST X: REQUESTINGS
#     Subtest: Posting a hit
#     Subtest: Registering a worker.
#     Subtest: Handling a request.
#     Subtest: Returning a task.
#     Subtest: Generating a task.
# """
# _log.info('Adding task %s to mturk as hit under hit type id %s')
# hid = mt.add_hit_to_hit_type(prac_tid, task_id)
#
# _log.info('Attempting to fetch mturk HIT object')
# hit = mt.get_hit(hid)

# ---------------------------------------------------------------------------- #

"""
This is designed to get everything into a usable state, with dbget, dbset, and mturk all initialized, and also loads the
JSON response template.
"""

from conf import *
from db import Get
from db import Set
import happybase
from glob import glob
import logging
import mturk
import os
import boto
import json
from generate import make_html
from generate import fetch_task
import ipdb
from time import sleep

conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate the database connection.
dbget = Get(conn)
dbset = Set(conn)

MTURK_ACCESS_ID = os.environ['MTURK_ACCESS_ID']
MTURK_SECRET_KEY = os.environ['MTURK_SECRET_KEY']

mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                               aws_secret_access_key=MTURK_SECRET_KEY,
                                               host='mechanicalturk.sandbox.amazonaws.com')

mt = mturk.MTurk(mtconn)

krypton_wid = 'A1RPGGKICRYDKC'

with open('/repos/mturk_task_v2/request', 'r') as f:
    response = json.load(f)

# make up a fake ip
ip = "127.0.0.1"
hit_type_id = "fake_hit_type_id"

dbset.task_finished_from_json(response, worker_ip=ip, hit_type_id=hit_type_id)

# task_id = 'p_YF4yu8WnS9z3VeoC'
# worker_id = 'A1RPGGKICRYDKC'
# make_html(task_id)
# fetch_task(dbget, dbset, task_id, worker_id=worker_id)

# ---------------------------------------------------------------------------- #

"""
This will reset everything back to its original state.
"""
from conf import *
from db import Get
from db import Set
import happybase
from glob import glob
import logging
import mturk
import os
import boto
import ipdb
from time import sleep

_log = logger.setup_logger('back to front test I')
_log.setLevel(logging.DEBUG)

_log.info('Connecting to database')
conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate the database connection.

_log.info('Instantiating db connections')
dbget = Get(conn)
dbset = Set(conn)

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
    # it appears that the incorrect qualification ids will be fetched if they're fetched too soon after being deleted.
    # to avoid this, we sleep for some amount of 60 sec after they've been deleted.

MTURK_ACCESS_ID = os.environ['MTURK_ACCESS_ID']
MTURK_SECRET_KEY = os.environ['MTURK_SECRET_KEY']


_log.info('Intantiating mturk connection')
mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                               aws_secret_access_key=MTURK_SECRET_KEY,
                                               host='mechanicalturk.sandbox.amazonaws.com')

# my_worker_id = 'A1RPGGKICRYDKC'
# mt = mturk.MTurk(mtconn)

# we don't need to cleanup mturk anymore, i'm very confident in the effectiveness of the create / identify
# qualifications functionality of the MTurk class
while True:
    try:
        cleanup_mturk(mtconn)
        break
    except:
        _log.warn('Qualification deletion error, please wait.')
        sleep(10)
        _log.info('Trying again...')

# testing params
n_images = 300
to_activate = 100

_log.info('Rebuilding tables')
dbset.force_regen_tables()

images = glob('static/images/*.jpeg')
_log.info('Found %i images' % len(images))
_log.info('Selecting %i to be input into the database' % n_images)

ims = list(np.random.choice(images, n_images, replace=False))

im_ids = [x.split('/')[-1].split('.')[0] for x in ims] # get the image ids
_log.info('Registering images...')
dbset.register_images(im_ids[:295], ims[:295])
dbset.register_images(im_ids[295:], ims[295:], attributes=['test'])

_log.info('Activating first 200 images...')
dbset.activate_images(im_ids[:200])

_log.info('Instantiating mturk object')
while True:
    try:
        mt = mturk.MTurk(mtconn)
        break
    except:
        _log.warn('Qualifications still exist, please wait.')
        sleep(10)
        _log.info('Trying again...')

_log.info('Disposing of all HITs')
mt.disable_all_hits_of_type()

_log.info('Disposing of all HIT types')
mt.dispose_of_hit_type()
