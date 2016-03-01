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
    which is is in
        $HBASE_HOME/bin

Also, to use the HBase running on the server but with a webserver running
locally you may wish to use an SSH Tunnel:

ssh -i ~/.ssh/mturk_stack_access.pem -L 9090:localhost:9090 ubuntu@<ip_addr>

where <ip_addr> is the location of the instance, i.e., 10.0.49.46

"""

from db import Get
from db import Set
from generate import fetch_task
from generate import make_success
from generate import make_practice_passed
from generate import make_practice_failed
from generate import make_practice_already_passed
from generate import make_preview_page
from mturk import MTurk
import boto.mturk.connection
import happybase
from conf import *
from flask import Flask
from flask import request
from workerpool import ThreadPool
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import statemon
import monitor

_log = logger.setup_logger(__name__)

# create state monitoring variables & handle statemon
mon = statemon.state
statemon.define("n_tasks_generated")
statemon.define("n_practices_generated")
statemon.define("n_workers_banned")
statemon.define("n_workers_unbanned")
statemon.define("n_tasks_accepted")
statemon.define("n_tasks_rejected")
statemon.define("n_tasks_served")
statemon.define("n_tasks_in_progress")
statemon.define("n_practices_rejected")
statemon.define("n_practices_passed")


if not CONTINUOUS_MODE:
    _log.warn('Not running in continuous mode: New tasks will not be posted')
else:
    _log.info('Running in continuous mode, will post tasks as long as there '
              'are funds available.')
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
    mean_seen = dbget.image_get_mean_seen(IMAGE_ATTRIBUTES)
    if mean_seen > MEAN_SAMPLES_REQ_PER_IMAGE(n_active):
        _log.info('Images are sufficiently sampled, activating more')
        dbset.activate_n_images(ACTIVATION_CHUNK_SIZE)
    hit_cost = DEFAULT_TASK_PAYMENT
    bal = mt.get_account_balance()
    if hit_cost > bal:
        _log.warn('Insufficient funds to generate new tasks: %.2f cost vs. '
                  '%.2f balance', hit_cost, bal)
        return
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
    mon.increment("n_tasks_generated")


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
    tot_cost = DEFAULT_PRACTICE_PAYMENT * NUM_ASSIGNMENTS_PER_PRACTICE * \
               to_generate
    bal = mt.get_account_balance()
    if tot_cost > bal:
        _log.warn('Insufficient funds to generate practicse: %.2f cost vs. '
                  '%.2f balance', tot_cost, bal)
        return
    for hit in practice_hits:
        if mt.get_practice_status(hit=hit) == PRACTICE_EXPIRED:
            _log.info('Practice %s expired' % hit.HITId)
            # disable it
            mt.disable_hit(hit.HITId)
            to_generate += 1
        elif mt.get_practice_status(hit=hit) == PRACTICE_COMPLETE:
            _log.info('Practice %s is complete' % hit.HITId)
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
        mon.increment('n_practices_generated')


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
        mon.increment('n_workers_banned')


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
    _log.info('Checking if any bans can be lifted...')
    # TODO: Get this for workers that are obtained from mturk, not the database
    for worker_id in dbget.get_all_workers():
        if not dbset.worker_ban_expires_in(worker_id):
            mt.unban_worker(worker_id)
            mon.increment("n_workers_unbanned")


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
    dbget = Get(conn)
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
    dbget = Get(conn)
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
    dbset.accept_task(task_id)


def handle_reject_task(mt, dbget, dbset, worker_id, assignment_id, task_id,
                       reason):
    """
    Handles a rejected task asynchronously, i.e., by being passed to the
    threadpool.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param worker_id: The worker ID
    :param assignment_id: The MTurk assignment ID
    :param task_id: The internal task ID
    :param reason: The reason for the rejection
    :return: None
    """
    mt.soft_reject_assignment(worker_id, assignment_id, reason)
    dbset.reject_task(task_id, reason)


def handle_finished_hit(mt, dbget, dbset, hit_id):
    """
    Disables a completed task asynchronously, i.e., by being passed to the
    threadpool.

    NOTES:
        Either an assignment_id or hit_id must be provided.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param hit_id: The reason for the rejection
    :return: None
    """
    _log.info('Currently not disabling completed hits due to the workers being'
              ' too slow to hit submit')
    # mt.disable_hit(hit_id)


"""
FLASK FUNCTIONS
"""


@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    """
    Returns a health check, since the task will be behind an ELB.

    :return: Health Check page
    """
    return "OK"


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
    task_id = hit_info.RequesterAnnotation
    if is_preview:
        is_practice = dbget.task_is_practice(task_id)
        task_time = dbget.get_task_time(task_id)
        return make_preview_page(is_practice, task_time)
    worker_id = request.values.get('workerId', '')
    response = fetch_task(dbget, dbset, task_id, worker_id)
    mon.increment("n_tasks_served")
    mon.increment("n_tasks_in_progress")
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
    if is_practice:
        mt.decrement_worker_practice_weekly_quota(worker_id)
        dbset.register_demographics(request.json, worker_ip)
        passed_practice = request.json[0]['passed_practice']
        if mt.get_worker_passed_practice(worker_id):
            to_return = make_practice_already_passed()
        elif passed_practice:
            to_return = make_practice_passed()
            dbset.practice_pass(request.json)
            mt.grant_worker_practice_passed(worker_id)
            mon.increment("n_practices_passed")
            mon.decrement("n_tasks_in_progress")
        else:
            to_return = make_practice_failed()
            mon.increment("n_practices_rejected")
            mon.decrement("n_tasks_in_progress")
    else:
        to_return = make_success()
        try:
            mt.decrement_worker_daily_quota(worker_id)
        except:
            _log.warn('Problem decrementing daily quota for %s' % worker_id)
        try:
            frac_contradictions, frac_unanswered, mean_rt, prob_random = \
                dbset.task_finished_from_json(request.json,
                                              hit_type_id=hit_type_id)
        except:
            _log.error('Problem storing task data - dumping task json')
            _log.info('TASK JSON: %s' % str(request.json))
            return to_return
        try:
            is_valid, reason = \
                dbset.validate_task(task_id=None,
                                    frac_contradictions=frac_contradictions,
                                    frac_unanswered=frac_unanswered,
                                    mean_rt=mean_rt, prob_random=prob_random)
        except:
            _log.error('Could not validate task, default to accept')
            is_valid = True
            reason = None
        if not is_valid:
            pool.add_task(handle_reject_task,
                          worker_id,
                          assignment_id,
                          task_id,
                          reason)
            pool.add_task(check_ban, worker_id)
            mon.increment("n_tasks_rejected")
            mon.decrement("n_tasks_in_progress")
        else:
            pool.add_task(handle_accepted_task, assignment_id, task_id)
            mon.increment("n_tasks_accepted")
            mon.decrement("n_tasks_in_progress")
        if CONTINUOUS_MODE:
            pool.add_task(create_hit, hit_type_id)
        pool.add_task(handle_finished_hit, hit_id)
    return to_return


context = ('%s/%s.crt' % (CERT_DIR, CERT_NAME), '%s/%s.key' % (CERT_DIR,
                                                               CERT_NAME))


if __name__ == '__main__':
    logger.config_root_logger('/repos/mturk_task_v2/logs/webserver.log')
    _log.info('Fetching hit types')
    # start the monitoring agent
    if not LOCAL:
        magent = monitor.MonitoringAgent()
        magent.start()
    PRACTICE_HIT_TYPE_ID = dbget.get_active_practice_hit_type_for(
        task_attribute=ATTRIBUTE,
        image_attributes=IMAGE_ATTRIBUTES)
    TASK_HIT_TYPE_ID = dbget.get_active_hit_type_for(
        task_attribute=ATTRIBUTE,
        image_attributes=IMAGE_ATTRIBUTES)
    if not PRACTICE_HIT_TYPE_ID or not TASK_HIT_TYPE_ID:
        if PRACTICE_HIT_TYPE_ID:
            dbset.deactivate_hit_type(PRACTICE_HIT_TYPE_ID)
        if TASK_HIT_TYPE_ID:
            dbset.deactivate_hit_type(TASK_HIT_TYPE_ID)
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
    if CONTINUOUS_MODE:
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
    scheduler.start()
    app.run(host='127.0.0.1', port=12344,
            debug=True, ssl_context=context)
    atexit.register(scheduler.shutdown)
    atexit.register(pool.wait_completion())
