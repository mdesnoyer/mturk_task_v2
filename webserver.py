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
        start-hbase.sh
    which is is in
        $HBASE_HOME/bin

    You also need to start thrift:
        nohup hbase thrift start &

Also, to use the HBase running on the server but with a webserver running
locally you may wish to use an SSH Tunnel:

ssh -i ~/.ssh/mturk_stack_access.pem -L 9000:localhost:9090 ubuntu@<ip_addr>

where <ip_addr> is the location of the instance, i.e., 10.0.36.202

then:
    happybase.Connection(host="localhost", port=9000)
"""

from db import Get
from db import Set
from generate import fetch_task
from generate import make_success
from generate import make_practice_passed
from generate import make_practice_failed
from generate import make_practice_already_passed
from generate import make_preview_page
from generate import make_error
from mturk import MTurk
import boto.mturk.connection
import happybase
from conf import *
from flask import Flask
from flask import request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
import statemon
import monitor
import boto.ses
import traceback
import logging


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
statemon.define("n_practices_rejected")
statemon.define("n_practices_passed")
statemon.define("n_errors_observed")
statemon.define("n_unknown_errors")


if not CONTINUOUS_MODE:
    _log.warn('Not running in continuous mode: New tasks will not be posted')
else:
    _log.info('Running in continuous mode, will post tasks as long as there '
              'are funds available.')

if LOCAL:
    _log.info('Running in local mode')
    EXTERNAL_QUESTION_ENDPOINT = 'https://127.0.0.1:12344/task'
    EXTERNAL_QUESTION_SUBMISSION_ENDPOINT = 'https://127.0.0.1:12344/submit'
# instantiate a database connection & database objects
_log.info('Instantiating database connection')
pool = happybase.ConnectionPool(size=8, host=DATABASE_LOCATION)
dbget = Get(pool)
dbset = Set(pool)

emconn = boto.ses.connect_to_region('us-east-1')

# instantiate the mechanical turk connection & mturk objects
_log.info('Instantiating mturk connection')

mtconn = boto.mturk.connection.MTurkConnection(
            aws_access_key_id=MTURK_ACCESS_ID,
            aws_secret_access_key=MTURK_SECRET_KEY,
            host=MTURK_HOST)

mt = MTurk(mtconn)
mt.setup_quals()

# instantiate the thread pool
executors = {'default': ThreadPoolExecutor(NUM_THREADS)}
job_defaults = {'misfire_grace_time': 999999}
scheduler = BackgroundScheduler(executors=executors,
                                job_defaults=job_defaults)

app = Flask(__name__)


"""
POOL FUNCTIONS
"""


def check_tasks(mt, dbget, dbset, hit_type_id):
    """
    Ensures that there is an appropriate number of tasks posted and active.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param hit_type_id: The HIT type ID, as a string.
    :return: None.
    """
    _log.info('JOB STARTED check_tasks: Looking for missing tasks')
    num_extant_hits = mt.get_all_pending_hits_of_type(
        TASK_HIT_TYPE_ID, ids_only=True)
    to_generate = max(NUM_TASKS - len(num_extant_hits), 0)
    if to_generate:
        _log.info('Building %i new tasks and posting them' % to_generate)
        for _ in range(to_generate):
            create_hit(mt, dbget, dbset, hit_type_id)


def create_hit(mt, dbget, dbset, hit_type_id):
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
    _log.info('JOB_STARTED create_hit')
    _log.info('Checking image statuses')
    dbget.update_sampling()
    if dbget.should_halt():
        return
    # n_active = dbget.get_n_active_images_count(IMAGE_ATTRIBUTES)
    # mean_seen = dbget.image_get_mean_seen(IMAGE_ATTRIBUTES)
    # if mean_seen > MEAN_SAMPLES_REQ_PER_IMAGE(n_active):
    #     _log.info('Images are sufficiently sampled, activating more')
    #     dbset.activate_n_images(ACTIVATION_CHUNK_SIZE)
    # else:
    #     _log.info('Mean %.2f < required %.2f' % (mean_seen,
    #                                              MEAN_SAMPLES_REQ_PER_IMAGE(
    #                                                  n_active)))
    hit_cost = DEFAULT_TASK_PAYMENT
    bal = mt.get_account_balance()
    if hit_cost > bal:
        _log.warn('Insufficient funds to generate new tasks: %.2f cost vs. '
                  '%.2f balance', hit_cost, bal)
        return
    if bal < LOW_FUNDS_WARNING:
        dispatch_notification('Low funds: %s' % str(bal),
                              subject="LOW BALANCE WARING")
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


def check_practices(mt, dbget, dbset, hit_type_id):
    """
    Checks to make sure that the practices are up, etc. If not, rebuilds them.

    NOTES:
        Right now, if the practices are all used up (but not expired!) then
        they are not re-created. They are only re-created upon expiration.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param hit_type_id: The HIT type ID, as a string.
    :return: None.
    """
    _log.info('JOB STARTED check_practices: Checking practices...')
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
    if bal < LOW_FUNDS_WARNING:
        dispatch_notification('Low funds: %s' % str(bal),
                              subject="LOW BALANCE WARING")
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
    if to_generate <= 0:
        return
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


def create_practice(mt, dbget, dbset, hit_type_id):
    """
    Mirrors the functionality of create_hit, only creates practices instead.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :param hit_type_id: The HIT type ID, as a string.
    :return: None.
    """
    _log.info('JOB_STARTED create_practice')
    if dbget.should_halt():
        return
    hit_cost = DEFAULT_PRACTICE_PAYMENT
    bal = mt.get_account_balance()
    if hit_cost > bal:
        _log.warn('Insufficient funds to generate new tasks: %.2f cost vs. '
                  '%.2f balance', hit_cost, bal)
        return
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
    _log.info('JOB STARTED check_ban')
    if dbget.worker_autoban_check(worker_id):
        dbset.ban_worker(worker_id)
        try:
            mt.ban_worker(worker_id)
        except Exception as e:
            tb = traceback.format_exc()
            dispatch_err(e, tb, None)
            return
        dispatch_notification('Worker %s has been banned' % str(worker_id),
                              subject="Ban notification")
        try:
            mon.increment('n_workers_banned')
        except:
            _log.warn('Could not increment statemons')


def unban_workers(mt, dbget, dbset):
    """
    Designed to run periodically, checks to see the workers -- if any -- that
    need to be unbanned.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :param dbset: A database Set object.
    :return: None
    """
    _log.info('JOB STARTED unban_workers')
    _log.info('Checking if any bans can be lifted...')
    for worker_id in dbget.get_all_workers():
        if dbget.worker_is_banned(worker_id):
            if not dbset.worker_ban_expires_in(worker_id):
                mt.unban_worker(worker_id)
                dispatch_notification('Worker %s has been unbanned' % str(
                    worker_id), subject="Unban notification")
                try:
                    mon.increment("n_workers_unbanned")
                except:
                    _log.warn('Could not increment statemons')


def reset_worker_quotas(mt, dbget):
    """
    Designed to run periodically, resets all the worker completion quotas.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :return: None
    """
    _log.info('JOB STARTED reset_worker_quotas')
    for worker_id in dbget.get_all_workers():
        mt.reset_worker_daily_quota(worker_id)


def reset_weekly_practices(mt, dbget):
    """
    Designed to run periodically, resets all the worker practice quotas.

    :param mt: A MTurk object.
    :param dbget: A database Get object.
    :return: None
    """
    _log.info('JOB STARTED reset_weekly_practices')
    for worker_id in dbget.get_all_workers():
        mt.reset_worker_weekly_practice_quota(worker_id)


def handle_accepted_task(dbset, task_id):
    """
    Handles an accepted task asynchronously, i.e., by being passed to the
    threadpool.

    :param dbset: A database Set object.
    :param task_id: The internal task ID
    :return: None
    """
    dbset.accept_task(task_id)


def handle_reject_task(mt, dbset, worker_id, assignment_id, task_id,
                       reason):
    """
    Handles a rejected task asynchronously, i.e., by being passed to the
    threadpool.

    :param mt: A MTurk object.
    :param dbset: A database Set object.
    :param worker_id: The worker ID
    :param assignment_id: The MTurk assignment ID
    :param task_id: The internal task ID
    :param reason: The reason for the rejection
    :return: None
    """
    _log.info('Soft rejection assignment %s from worker %s: %s' % (
        assignment_id, worker_id, reason))
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
    # mt.disable_hit(hit_id)
    pass


"""
HELPER FUNCTIONS
"""


def shutdown_server():
    """
    Function to shutdown the server.
    """
    scheduler.shutdown(wait=False)
    func = request.environ.get('werkzeug.server.shutdown')
    if func is None:
        raise RuntimeError('Not running with the Werkzeug Server')
    func()


def dispatch_err(e, tb='', request=None):
    """
    Dispatches an error email.

    :param e: The exception.
    :param tb: The traceback.
    :param request: The Flask request.
    """
    try:
        mon.increment('n_errors_observed')
    except:
        _log.warn('Could not increment observed error count')
    if request is not None:
        if request.headers.getlist("X-Forwarded-For"):
           src = request.headers.getlist("X-Forwarded-For")[0]
        else:
           src = request.remote_addr
        try:
            req_json = request.json
        except:
            req_json = 'No JSON'
        try:
            ua = str(request.user_agent)
        except:
            ua = 'No User Agent'
    else:
        src = 'No request'
        req_json = 'No request'
        ua = 'No request'
    subj = 'MTurk Error %s' % e.message
    body_elem = ['Error: %s' % e.message,
                 'Traceback:\n%s' % tb,
                 'IP: %s' % src,
                 'User Agent: %s' % ua,
                 'JSON: %s' % str(req_json)]
    body = '\n\n----------------------------\n\n'.join(body_elem)
    emconn.send_email('ops@kryto.me', subj, body,
                      ['kryptonlabs99@gmail.com'])


def dispatch_notification(message, subject='Notification'):
    """
    Dispatches a message to kryptonlabs99@gmail.com

    :param message: The body of the message.
    :param subject: The subject of the message.
    """
    emconn.send_email('ops@kryto.me', subject, message,
                      ['kryptonlabs99@gmail.com'])


"""
FLASK FUNCTIONS
"""


@app.errorhandler(500)
def internal_error(e):
    try:
        mon.increment('n_unknown_errors')
    except:
        _log.warn('Could not increment unknown error count')
    try:
        _log.warn('Internal server error.')
        dispatch_err(e)
    except:
        pass
    try:
        return make_error('Unknown internal server error.')
    except:
        return 'Error 500 Otherwise Unknown'


shutdown_url = rand_id_gen(15)
shutdown_endpoint = 'mturk.kryto.me/%s' % shutdown_url
stopaddition_url = rand_id_gen(15)
stopaddition_endpoint = 'mturk.kryto.me/%s' % stopaddition_url
halt_url = rand_id_gen(15)
halt_endpoint = 'mturk.kryto.me/%s' % halt_url


@app.route('/%s' % shutdown_url, methods=['GET', 'POST'])
def shutdown():
    """
    Shuts down the server.

    :return: None
    """
    if request.headers.getlist("X-Forwarded-For"):
        src = request.headers.getlist("X-Forwarded-For")[0]
    else:
        src = request.remote_addr
    dispatch_notification(str(src), subject='Mturk task shutdown')
    _log.error('Shutdown request received from %s' % str(src))
    # _log.info('Disposing of all HITs')
    shutdown_server()
    # mt.disable_all_hits_of_type()
    return 'Server shutting down...'


@app.route('/%s' % stopaddition_url, methods=['GET', 'POST'])
def stopaddition():
    """
    Halts the addition of new tasks and practices.

    :return: None
    """
    if request.headers.getlist("X-Forwarded-For"):
        src = request.headers.getlist("X-Forwarded-For")[0]
    else:
        src = request.remote_addr
    dispatch_notification(str(src), subject='Mturk task addition stopped')
    _log.warn('Stopping addition')
    global CONTINUOUS_MODE
    CONTINUOUS_MODE = False
    return 'Continuous mode disabled'


@app.route('/%s' % halt_url, methods=['GET', 'POST'])
def halt():
    """
    Halts the production of new hits and deletes old ones.

    :return: None
    """
    if request.headers.getlist("X-Forwarded-For"):
        src = request.headers.getlist("X-Forwarded-For")[0]
    else:
        src = request.remote_addr
    dispatch_notification(str(src), subject='Mturk task halted')
    _log.warn('Halting!')
    global CONTINUOUS_MODE
    CONTINUOUS_MODE = False
    mt.disable_all_hits_of_type()
    return 'HITs disabled, continuous mode disabled'

body = 'Stop Endpoint: %s\nHalt Endpoint: %s\nShutdown Endpoint: %s\n'
body = body % (stopaddition_endpoint, halt_endpoint, shutdown_endpoint)
dispatch_notification(body, subject='Control Endpoints')


@app.route('/healthcheck', methods=['GET'])
def healthcheck():
    """
    Returns a health check, since the task will be behind an ELB.

    :return: Health Check page
    """
    # src = request.remote_addr
    # _log.debug('Healthcheck request from %s received' % str(src))
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
    if request.headers.getlist("X-Forwarded-For"):
        src = request.headers.getlist("X-Forwarded-For")[0]
    else:
        src = request.remote_addr
    is_preview = request.values.get('assignmentId', '') == PREVIEW_ASSIGN_ID
    hit_id = request.values.get('hitId', None)
    if hit_id is None:
        _log.debug('Returning request to %s' % str(src))
        return make_error('Could not fetch HIT ID.')
    try:
        val_hit_info = mtconn.get_hit(hit_id)[0]
    except:
        body = 'Unassignable HIT requested: %s'
        body = body % str(hit_id)
        subject = body
        dispatch_notification(body, subject)
        return 'Could not confirm request with MTurk'
    try:
        assert val_hit_info.HITStatus == 'Unassignable'
    except:
        body = 'HIT %s accepted but is not unassignable. status: %s'
        body  = body % (str(hit_id), str(val_hit_info.HITStatus))
        subject = body
        dispatch_notification(body, subject)
        return 'Apologies, this HIT is %s' % str(val_hit_info.HITStatus)
    try:
        hit_info = mt.get_hit(hit_id)
        task_id = hit_info.RequesterAnnotation
    except Exception as e:
        tb = traceback.format_exc()
        dispatch_err(e, tb, request)
        err_dict = {'HIT ID': hit_id}
        return make_error('Could not fetch HIT information.',
                          error_data=err_dict, hit_id=hit_id)
    is_practice = False
    if len(task_id) >= len(PRACTICE_PREFIX):
        if task_id[:len(PRACTICE_PREFIX)] == PRACTICE_PREFIX:
            is_practice = True
    if is_preview:
        if is_practice:
            task_time = dbget.practice_time
            _log.debug('Returning practice preview request from %s' % str(src))
        else:
            task_time = dbget.task_time
            _log.debug('Returning task preview request from %s' % str(src))
        return make_preview_page(is_practice, task_time)
    worker_id = request.values.get('workerId', '')
    if is_practice:
        # check if they have the practice quota qualification
        pq_id = mt.practice_quota_id
        try:
            mtconn.get_qualification_score(pq_id, worker_id)
        except:  # ahh this isn't a real worker! KILL THEM!
            body = 'Unknown worker %s tried to request a practice.'
            body = body % worker_id
            subject = body
            dispatch_notification(body, subject)
            return 'Could not confirm request with MTurk.'
    else:
        # check that they have the daily quota qualification
        pt_id = mt.quota_id
        try:
            mtconn.get_qualification_score(pt_id, worker_id)
        except:  # ahh this isn't a real worker! KILL THEM!
            body = 'Unknown worker %s tried to request a task.'
            body = body % worker_id
            subject = body
            dispatch_notification(body, subject)
            return 'Could not confirm request with MTurk.'
    try:
        response = fetch_task(dbget, dbset, task_id, worker_id, is_practice)
    except Exception as e:
        tb = traceback.format_exc()
        dispatch_err(e, tb, request)
        return make_error('Could not fetch HIT, this HIT has probably been '
                          'removed.',
                          error_data={'HIT ID': hit_id, 'TASK ID': task_id,
                                      'WORKER ID': worker_id},
                          hit_id=hit_id, task_id=task_id)
    try:
        mon.increment("n_tasks_served")
    except Exception as e:
        _log.warn('Could not increment statemons: %s' % e.message)
    return response


@app.route('/submit', methods=['POST', 'GET'])
def submit():
    """
    Allows a user to submit a task, and inputs all the relevant data into the
    database.

    :return: Success page.
    """
    try:
        if request.headers.getlist("X-Forwarded-For"):
            worker_ip = request.headers.getlist("X-Forwarded-For")[0]
        else:
            worker_ip = request.remote_addr
        hit_id = request.json[0]['hitId']
        worker_id = request.json[0]['workerId']
        task_id = request.json[0]['taskId']
        assignment_id = request.json[0]['assignmentId']
        hit_info = mt.get_hit(hit_id)
    except Exception as e:
        tb = traceback.format_exc()
        dispatch_err(e, tb, request)
        return make_error('Problem fetching submission information.')
    err_dict = {'HIT ID': hit_id, 'WORKER ID': worker_id, 'TASK ID': task_id,
                'ASSIGNMENT ID': assignment_id}
    try:
        hit_type_id = hit_info.HITTypeId
    except AttributeError as e:
        _log.warn('No HIT type ID associated with hit %s' % hit_id)
        hit_type_id = ''
    is_practice = request.json[0]['is_practice']
    if is_practice:
        # ---------- Handle submitted practice task ----------
        try:
            mt.decrement_worker_practice_weekly_quota(worker_id)
        except Exception as e:
            _log.warn('Problem decrementing worker weekly practice quota for '
                      '%s: %s', worker_id, e.message)
            tb = traceback.format_exc()
            dispatch_err(e, tb, request)
        try:
            dbset.register_demographics(request.json, worker_ip)
        except Exception as e:
            tb = traceback.format_exc()
            dispatch_err(e, tb, request)
        passed_practice = request.json[0]['passed_practice']
        if mt.get_worker_passed_practice(worker_id):
            to_return = make_practice_already_passed(hit_id=hit_id,
                                                     task_id=task_id)
        elif passed_practice:
            try:
                to_return = make_practice_passed(hit_id=hit_id,
                                                 task_id=task_id)
            except Exception as e:
                tb = traceback.format_exc()
                dispatch_err(e, tb, request)
                return make_error('Error creating practice passed page',
                                  error_data=err_dict, hit_id=hit_id,
                                  task_id=task_id, allow_submit=True)
            mt.grant_worker_practice_passed(worker_id)
            try:
                mon.increment("n_practices_passed")
            except Exception as e:
                _log.warn('Could not increment statemons: %s' % e.message)
        else:
            try:
                to_return = make_practice_failed(hit_id=hit_id,
                                                 task_id=task_id)
            except Exception as e:
                tb = traceback.format_exc()
                dispatch_err(e, tb, request)
                return make_error('Error creating practice passed page',
                                  error_data=err_dict, hit_id=hit_id,
                                  task_id=task_id, allow_submit=True)
            try:
                mon.increment("n_practices_rejected")
            except Exception as e:
                _log.warn('Could not increment statemons: %s' % e.message)
        if CONTINUOUS_MODE:
            scheduler.add_job(create_practice,
                              args=[mt, dbget, dbset, hit_type_id])
    else:
        # ---------- Handle submitted task ---------- #
        try:
            dbset.validate_demographics(request.json)
        except Exception as e:
            tb = traceback.format_exc()
            dispatch_err(e, tb, request)
        try:
            to_return = make_success(hit_id=hit_id,
                                     task_id=task_id)
        except Exception as e:
            tb = traceback.format_exc()
            dispatch_err(e, tb, request)
            return make_error('Error creating submit page.',
                              error_data=err_dict, hit_id=hit_id,
                              task_id=task_id, allow_submit=True)
        try:
            mt.decrement_worker_daily_quota(worker_id)
        except Exception as e:
            _log.error('Problem decrementing daily quota: %s' % e.message)
            tb = traceback.format_exc()
            dispatch_err(e, tb, request)
        try:
            frac_contradictions, frac_unanswered, frac_too_fast, prob_random = \
                dbset.task_finished_from_json(request.json,
                                              hit_type_id=hit_type_id,
                                              user_agent=request.user_agent)
            _log.debug('Assignment %s submitted from %s:\n\tFraction '
                       'contractions: %.2f\n\tFraction unanswered: '
                       '%.2f\n\tFraction too fast: %.2f\n\tChi Square score: '
                       '%.2f' % (assignment_id, worker_id,
                                 frac_contradictions, frac_unanswered,
                                 frac_too_fast, prob_random))
        except Exception as e:
            _log.error('Problem storing task data: %s' % e.message)
            tb = traceback.format_exc()
            dispatch_err(e, tb, request)
            return to_return
        try:
            is_valid, reason = \
                dbset.validate_task(task_id=None,
                                    frac_contradictions=frac_contradictions,
                                    frac_unanswered=frac_unanswered,
                                    frac_too_fast=frac_too_fast,
                                    prob_random=prob_random)
        except Exception as e:
            _log.error('Could not validate task, default to accept. Error '
                       'was: %s' % e.message)
            tb = traceback.format_exc()
            dispatch_err(e, tb, request)
            is_valid = True
            reason = None
        if not is_valid:
            scheduler.add_job(handle_reject_task,
                              args=[mt, dbset, worker_id,
                                    assignment_id, task_id, reason])
            scheduler.add_job(check_ban,
                              args=[mt, dbget, dbset, worker_id])
            try:
                mon.increment("n_tasks_rejected")
            except Exception as e:
                _log.warn('Could not increment statemons: %s' % e.message)
        else:
            scheduler.add_job(handle_accepted_task,
                              args=[dbset, task_id])
            try:
                mon.increment("n_tasks_accepted")
            except Exception as e:
                _log.warn('Could not increment statemons: %s' % e.message)
        if CONTINUOUS_MODE:
            scheduler.add_job(create_hit, args=[mt, dbget, dbset, hit_type_id])
        scheduler.add_job(handle_finished_hit, args=[mt, dbget, dbset, hit_id])
    return to_return


if __name__ == '__main__':
    webhand = logger.config_root_logger(LOG_LOCATION, return_webserver=True)
    app.logger.addHandler(webhand)
    # start the monitoring agent
    dbget.check_active_ims()
    mins, secs = divmod(dbget.task_time, 60)  # prefetch the task time
    _log.info('Starting scheduler')
    scheduler.start()
    if not LOCAL:
        magent = monitor.MonitoringAgent()
        magent.start()
    _log.info('Fetching hit types')
    PRACTICE_HIT_TYPE_ID = dbget.get_active_practice_hit_type_for(
        task_attribute=ATTRIBUTE,
        image_attributes=IMAGE_ATTRIBUTES)
    TASK_HIT_TYPE_ID = dbget.get_active_hit_type_for(
        task_attribute=ATTRIBUTE,
        image_attributes=IMAGE_ATTRIBUTES)
    if not PRACTICE_HIT_TYPE_ID or not TASK_HIT_TYPE_ID:
        _log.info('Calculating payment')
        _task_payment = ((1./60) * dbget.task_time) * PAYMENT_PER_MIN
        task_payment = float(int(_task_payment * 100))/100
        mins, secs = divmod(dbget.task_time, 60)
        _log.info('Average task time is %i min, %i sec. Payment is %.2f',
                  int(mins), int(secs), task_payment)
        if PRACTICE_HIT_TYPE_ID:
            dbset.deactivate_hit_type(PRACTICE_HIT_TYPE_ID)
        if TASK_HIT_TYPE_ID:
            dbset.deactivate_hit_type(TASK_HIT_TYPE_ID)
        TASK_HIT_TYPE_ID, PRACTICE_HIT_TYPE_ID = \
            mt.register_hit_type_mturk(reward=task_payment)
        dbset.register_hit_type(TASK_HIT_TYPE_ID, reward=task_payment)
        dbset.register_hit_type(PRACTICE_HIT_TYPE_ID, is_practice=True)
    scheduler.add_job(check_practices,
                      args=[mt, dbget, dbset, PRACTICE_HIT_TYPE_ID])
    scheduler.add_job(check_tasks, args=[mt, dbget, dbset, TASK_HIT_TYPE_ID])
    # note that this must be done *after* the tasks are generated, since it
    # is the tasks that actually activate new images.
    scheduler.add_job(unban_workers, 'interval', hours=24,
                      args=[mt, dbget, dbset], id='unban workers')
    scheduler.add_job(reset_worker_quotas, 'cron', hour='0',
                      args=[mt, dbget], id='task quota reset')
    scheduler.add_job(reset_weekly_practices, 'cron', day_of_week='sun',
                      hour='1', args=[mt, dbget], id='practice quota reset')
    _log.info('Tasks being served on %s' % EXTERNAL_QUESTION_ENDPOINT)
    _log.info('Starting webserver')
    if LOCAL:
        CERT_NAME = 'server'
        CERT_DIR = 'certificates'
        context = ('%s/%s.crt' % (CERT_DIR, CERT_NAME),
                   '%s/%s.key' % (CERT_DIR, CERT_NAME))
        app.run(host='127.0.0.1', port=WEBSERVER_PORT,
                debug=True, ssl_context=context)
    else:
        app.run(host='0.0.0.0', port=WEBSERVER_PORT, threaded=True,
                debug=True, use_reloader=False)
