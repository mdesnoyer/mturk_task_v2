"""
This script actually runs the webserver. There are only two endpoints:
    request     - requests a task from the server
    submit      - submits a completed task to the server

This script also instantiates and distributes the Mechanical Turk connection
object (mtconn) and the HBase / HappyBase database connection object (conn).
Thus, this is the central routing house for commands -- it coordinates requests
and updates to data both on the database (dbget / dbset) and mturk (mturk).

Finally, this script exports the functions that will be executed
asynchronously by the threadpool. It also schedules events to be run by the
threadpool that do not need to be queued.

NOTES:
    For the mturk connection, make sure to export the AWS connection credentials
    in ~/.boto.


    To start HBase:
        hbase-start.sh

    To start thrift:
        hbase-daemon.sh start thrift

"""

from db import Get
from db import Set
from generate import fetch_task
from generate import make_success
from mturk import MTurk
import boto.mturk.connection
import happybase
from conf import *
from flask import Flask
from flask import request
from workerpool import ThreadPool
from apscheduler.schedulers.background import BackgroundScheduler

_log = logger.setup_logger(__name__)

# instantiate a database connection & database objects
_log.info('Intantiating database connection')
conn = happybase.Connection(host=DATABASE_LOCATION)
dbget = Get(conn)
dbset = Set(conn)

# instantiate the mechanical turk connection & mturk objects
_log.info('Intantiating mturk connection')
if MTURK_SANDBOX:
    mturk_host = MTURK_SANDBOX_HOST
else:
    mturk_host = MTURK_HOST

mtconn = boto.mturk.connection.MTurkConnection(
            aws_access_key_id=MTURK_ACCESS_ID,
            aws_secret_access_key=MTURK_SECRET_KEY,
            host=mturk_host)

mt = MTurk(mtconn)

# instantiate the thread pool
pool = ThreadPool(NUM_THREADS)

# identify the certificates
CERT_NAME = 'server'
CERT_DIR = 'certificates'

app = Flask(__name__)

# list of resources
resources = [
    'resources/instr_screenshots/accept_1.jpg',
    'resources/instr_screenshots/accept_2.jpg',
    'resources/instr_screenshots/reject_1.jpg',
    'resources/instr_screenshots/reject_1.jpg',
    'resources/templates/symbols/error.png',
    'resources/templates/symbols/check.png'
]
scripts = [  # note, this also includes the jsPsych-specific CSS.
    'js/jspsych-4.3/js/jquery.min.js',
    'js/jspsych-4.3/js/jquery-ui.min.js',
    'js/jspsych-4.3/jspsych.js',
    'js/jspsych-4.3/plugins/jspsych-click-choice.js',
    'js/jspsych-4.3/plugins/jspsych-instructions.js',
    'js/jspsych-4.3/plugins/jspsych-html.js',
    'js/practice_debrief.js',
    'js/progressbar.min.js',
    'js/jspsych-4.3/css/jspsych.css',
    'js/jspsych-4.3/css/jquery-ui.css'
]


def _get_static_urls():
    """
    Accepts a request for a task, and then returns the static URLs pointing to
    all the resources.

    NOTES
        The template variables corresponding to the resources are generally
        named with their filename (no directory or folder information) +
        their extension.

        Getting this to work with Flask is somewhat opaque. Even though Flask
        is the most lightweight web framework that I can find, it seems
        ridiculously overpowered for what I'm doing. Thus, _get_static_url's
        will just return the hard-coded stuff for now.

    :return: A dictionary of static urls, of the form
            {'resource_name': 'resource_url'}
    """
    static_urls = dict()

    for resource in resources:
        static_urls[
            os.path.basename(resource).replace('.', '_').replace('-', '_')] = \
            os.path.join('static', resource)
    for script in scripts:
        static_urls[
            os.path.basename(script).replace('.', '_').replace('-', '_')] = \
            os.path.join('static', script)
    static_urls['demographics'] = 'static/html/demographics.html'
    static_urls['success'] = 'static/html/success.html'
    static_urls['submit'] = EXTERNAL_QUESTION_SUBMISSION_ENDPOINT
    static_urls['attribute'] = ATTRIBUTE
    return static_urls

static_urls = _get_static_urls()


"""
POOL FUNCTIONS
"""


def create_hit(mt, dbget, dbset, hit_type_id=None):
    """
    The background task for creating new hits, which enables us to maintain a
    constant number of tasks at all times. Note that this should be only used
    for generating 'real' tasks--i.e., NOT practices!

    Additionally, this checks to make sure that an adequate number of images
    have been activated.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param hit_type_id: The HIT type ID, as a string.
    :return: None.
    """
    _log.info('Checking image statuses')
    n_active = dbget.get_n_active_images_count(IMAGE_ATTRIBUTES)
    min_seen = dbget.image_get_min_seen(IMAGE_ATTRIBUTES)
    if min_seen > SAMPLES_REQ_PER_IMAGE(n_active):
        _log.info('Images are sufficiently sampled, activating more')
        dbset.activate_n_images(ACTIVATION_CHUNK_SIZE)
    _log.info('Generating a new HIT')
    task_id, exp_seq, attribute, register_task_kwargs = \
        dbget.gen_task(DEF_NUM_IMAGES_PER_TASK, 3,
                       DEF_NUM_IMAGE_APPEARANCE_PER_TASK, n_keep_blocks=1,
                       n_reject_blocks=1, hit_type_id=hit_type_id)
    _log.info('Registering task in the database')
    dbset.register_task(task_id, exp_seq, attribute, **register_task_kwargs)
    _log.info('Adding task %s to mturk as hit under hit type id %s' % (
        task_id, hit_type_id))
    hid = mt.add_hit_to_hit_type(hit_type_id, task_id)
    _log.info('Hit %s is ready.' % hid)


def check_practices(hit_type_id=None):
    """
    Checks to make sure that the practices are up, etc. If not, rebuilds them.

    NOTES:
        Right now, if the practices are all used up (but not expired!) then
        they are not re-created. They are only re-created upon expiration.

    :param hit_type_id: The HIT type ID, as a string.
    :return: None.
    """
    conn = happybase.Connection(host=DATABASE_LOCATION)
    mtconn = boto.mturk.connection.MTurkConnection(
            aws_access_key_id=MTURK_ACCESS_ID,
            aws_secret_access_key=MTURK_SECRET_KEY,
            host=mturk_host)
    mt = MTurk(mtconn)
    dbget = Get(conn)
    dbset = Set(conn)
    _log.info('Checking practices...')
    to_generate = 0
    practice_hits = mt.get_all_hits_of_type(hit_type_id=hit_type_id)
    to_generate += NUM_PRACTICES - len(practice_hits)
    for hit in practice_hits:
        if mt.get_practice_status(hit=hit) == PRACTICE_EXPIRED:
            _log('Practice %s expired' % hit.HITId)
            # disable it
            mt.disable_hit(hit.HITId)
            to_generate += 1
        elif mt.get_practice_status(hit=hit) == PRACTICE_COMPLETE:
            _log('Practice %s is complete' % hit.HITId)
            # disable it
            mt.disable_hit(hit.HITId)
            to_generate += 1
    _log.info('Need to generate %i more practices' % to_generate)
    for _ in range(to_generate):
        task_id, exp_seq, attribute, register_task_kwargs = \
            dbget.gen_task(DEF_PRACTICE_NUM_IMAGES_PER_TASK, 3,
                           DEF_NUM_IMAGE_APPEARANCE_PER_TASK, n_keep_blocks=1,
                           n_reject_blocks=1, hit_type_id=hit_type_id,
                           practice=True)
        dbset.register_task(task_id, exp_seq, attribute,
                            **register_task_kwargs)
        mt.add_practice_hit_to_hit_type(hit_type_id, task_id)


def check_ban(mt, dbget, dbset, worker_id=None):
    """
    Checks to see if a worker needs to be banned

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param worker_id: The worker ID, as a string
    :return: None
    """
    if dbget.worker_autoban_check(worker_id):
        dbset.ban_worker(worker_id)
        mt.ban_worker(worker_id)


def unban_workers():
    """
    Designed to run periodically, checks to see the workers -- if any -- that
    need to be unbanned.

    :return: None
    """
    conn = happybase.Connection(host=DATABASE_LOCATION)
    mtconn = boto.mturk.connection.MTurkConnection(
            aws_access_key_id=MTURK_ACCESS_ID,
            aws_secret_access_key=MTURK_SECRET_KEY,
            host=mturk_host)
    mt = MTurk(mtconn)
    dbset = Set(conn)
    # TODO: Finally figure out the damn filters
    _log.info('Checking if any bans can be lifted...')
    for worker_id in dbget.get_all_workers():
        if not dbset.worker_ban_expires_in(worker_id):
            # TODO: Only unban if the worker is banned in the first place!
            mt.unban_worker(worker_id)


def reset_worker_quotas():
    """
    Designed to run periodically, resets all the worker completion quotas.

    :return: None
    """
    conn = happybase.Connection(host=DATABASE_LOCATION)
    mtconn = boto.mturk.connection.MTurkConnection(
            aws_access_key_id=MTURK_ACCESS_ID,
            aws_secret_access_key=MTURK_SECRET_KEY,
            host=mturk_host)
    mt = MTurk(mtconn)
    dbget = Set(conn)
    for worker_id in dbget.get_all_workers():
        mt.reset_worker_daily_quota(worker_id)


def reset_weekly_practices():
    """
    Designed to run periodically, resets all the worker practice quotas.

    :return: None
    """
    conn = happybase.Connection(host=DATABASE_LOCATION)
    mtconn = boto.mturk.connection.MTurkConnection(
            aws_access_key_id=MTURK_ACCESS_ID,
            aws_secret_access_key=MTURK_SECRET_KEY,
            host=mturk_host)
    mt = MTurk(mtconn)
    dbget = Set(conn)
    for worker_id in dbget.get_all_workers():
        mt.reset_worker_weekly_practice_quota(worker_id)


def handle_accepted_task(mt, dbget, dbset, assignment_id, task_id):
    """
    Handles an accepted task asynchronously, i.e., by being passed to the
    threadpool.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param assignment_id: The MTurk assignment ID
    :param task_id: The internal task ID
    :return: None
    """
    mt.approve_assignment(assignment_id)
    dbset.accept_task(task_id)


def handle_reject_task(mt, dbget, dbset, assignment_id, task_id, reason):
    """
    Handles a rejected task asynchronously, i.e., by being passed to the
    threadpool.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param assignment_id: The MTurk assignment ID
    :param task_id: The internal task ID
    :param reason: The reason for the rejection
    :return: None
    """
    mt.soft_reject_assignment(assignment_id, reason)
    dbset.reject_task(task_id, reason)

"""
FLASK FUNCTIONS
"""


@app.route('/task', methods=['POST', 'GET'])
def task():
    """
    Accepts a request for a task, and then returns the static URLs pointing to
    all the resources.

    NOTES
        The template variables corresponding to the resources are generally
        named with their filename (no directory or folder information) +
        their extension.

    :return: The Task / Practice / Error page / etc HTML.
    """
    is_preview = request.values.get('assignmentId') == PREVIEW_ASSIGN_ID
    hit_id = request.values.get('hitId', '')
    hit_info = mt.get_hit(hit_id)
    # TODO: Get HIT information.
    if not is_preview:
        worker_id = request.values.get('workerId', '')
    else:
        worker_id = None
    task_id = hit_info.RequesterAnnotation
    response = fetch_task(dbget, dbset, task_id, worker_id,
                          is_preview=is_preview, static_urls=static_urls)
    return response


@app.route('/submit', methods=['POST', 'GET'])
def submit():
    """
    Allows a user to submit a task, and inputs all the relevant data into the
    database.

    :return: Success page.
    """
    worker_ip = request.remote_addr
    hit_id = request.json[0]['hitId']
    worker_id = request.json[0]['workerId']
    task_id = request.json[0]['taskId']
    assignment_id = request.json[0]['assignmentId']
    hit_info = mt.get_hit(hit_id)
    try:
        hit_type_id = hit_info.HITTypeId
    except AttributeError as e:
        _log.warn('No HIT type ID associated with hit %s' % hit_id)
        hit_type_id = ''
    is_practice = request.json[0]['is_practice']
    to_return = make_success(static_urls)
    if is_practice:
        mt.decrement_worker_practice_weekly_quota(worker_id)
        dbset.register_demographics(request.json, worker_ip)
        passed_practice = request.json[0]['passed_practice']
        if passed_practice:
            dbset.practice_pass(request.json)
            mt.grant_worker_practice_passed(worker_id)
    else:
        mt.decrement_worker_daily_quota(worker_id)
        frac_contradictions, frac_unanswered, mean_rt, prob_random = \
            dbset.task_finished_from_json(request.json,
                                          hit_type_id=hit_type_id)
        is_valid, reason = \
            dbset.validate_task(task_id=None,
                                frac_contradictions=frac_contradictions,
                                frac_unanswered=frac_unanswered,
                                mean_rt=mean_rt, prob_random=prob_random)
        if not is_valid:
            pool.add_task(handle_reject_task, assignment_id, task_id, reason)
            pool.add_task(check_ban, worker_id)
        else:
            pool.add_task(handle_accepted_task, assignment_id, task_id)
        pool.add_task(create_hit, hit_type_id)
    return to_return


context = ('%s/%s.crt' % (CERT_DIR, CERT_NAME), '%s/%s.key' % (CERT_DIR,
                                                               CERT_NAME))


if __name__ == '__main__':
    _log.info('Fetching hit types')
    PRACTICE_HIT_TYPE_ID = dbget.get_active_practice_hit_type_for(
        task_attribute=ATTRIBUTE,
        image_attributes=IMAGE_ATTRIBUTES)
    TASK_HIT_TYPE_ID = dbget.get_active_hit_type_for(
        task_attribute=ATTRIBUTE,
        image_attributes=IMAGE_ATTRIBUTES)
    if not PRACTICE_HIT_TYPE_ID or not TASK_HIT_TYPE_ID:
        # TODO: if only one of them is defined, deactivate it.
        TASK_HIT_TYPE_ID, PRACTICE_HIT_TYPE_ID = mt.register_hit_type_mturk()
        dbset.register_hit_type(TASK_HIT_TYPE_ID)
        dbset.register_hit_type(PRACTICE_HIT_TYPE_ID, is_practice=True)
    _log.info('Looking for missing tasks')
    num_extant_hits = mt.get_all_pending_hits_of_type(
        TASK_HIT_TYPE_ID, ids_only=True)
    to_generate = max(NUM_TASKS - len(num_extant_hits), 0)
    if to_generate:
        _log.info('Building %i new tasks and posting them' % to_generate)
        for _ in range(to_generate):
            pool.add_task(create_hit, TASK_HIT_TYPE_ID)
    # note that this must be done *after* the tasks are generated, since it
    # is the tasks that actually activate new images.
    _log.info('Checking practice validity')
    check_practices(hit_type_id=PRACTICE_HIT_TYPE_ID)
    _log.info('Starting scheduler')
    scheduler = BackgroundScheduler()
    scheduler.add_job(check_practices,
                      trigger='interval',
                      minutes=60*60*3,                  # every 3 hours
                      args=[PRACTICE_HIT_TYPE_ID],
                      id='practice check')
    scheduler.add_job(unban_workers,
                      trigger='interval',
                      minutes=60*60*24,                 # every 24 hours
                      id='unban workers')
    scheduler.add_job(reset_worker_quotas,
                      trigger='interval',
                      minutes=60*60*24,                 # every 24 hours
                      id='task quota reset')
    scheduler.add_job(reset_weekly_practices,
                      trigger='interval',
                      minutes=60*60*24*7,               # every 7 days
                      id='practice quota reset')
    _log.info('Starting webserver')
    app.run(host='127.0.0.1', port=12344,
            debug=False, ssl_context=context)