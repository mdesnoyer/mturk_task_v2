"""
This script actually runs the webserver. There are only two endpoints:
    request     - requests a task from the server
    submit      - submits a completed task to the server

This script also instantiates and distributes the Mechanical Turk connection object (mtconn) and the HBase / HappyBase
database connection object (conn). Thus, this is the central routing house for commands -- it coordinates requests and
updates to data both on the database (dbget / dbset) and mturk (mturk).

NOTES:
    For the mturk connection, make sure to export the AWS connection credentials in ~/.boto.
"""
# TODO: determine if the AWS credentials can be stored locally, and not dependent on the ~ directory.

from db import Get
from db import Set
from generate import fetch_task
# from generate import make_demographics
from mturk import MTurk
# from daemon import Daemon
import boto.mturk.connection
import happybase
from conf import *
from flask import Flask
from flask import request
# from flask import url_for

_log = logger.setup_logger(__name__)

conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate the database connection.
if MTURK_SANDBOX:
    mturk_host = MTURK_SANDBOX_HOST
else:
    mturk_host = MTURK_HOST
MTURK_ACCESS_ID = os.environ['MTURK_ACCESS_ID']
MTURK_SECRET_KEY = os.environ['MTURK_SECRET_KEY']


_log.info('Intantiating mturk connection')
mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                               aws_secret_access_key=MTURK_SECRET_KEY,
                                               host='mechanicalturk.sandbox.amazonaws.com')

dbget = Get(conn)
dbset = Set(conn)
mt = MTurk(mtconn)

CERT_NAME = 'server'
CERT_DIR = '/repos/mturk_task_v2/certificates'


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
    Accepts a request for a task, and then returns the static URLs pointing to all the resources.

    NOTES
        The template variables corresponding to the resources are generally named with their filename (no directory or
        folder information) + their extension.

        Getting this to work with Flask is somewhat opaque. Even though Flask is the most lightweight web framework
        that I can find, it seems ridiculously overpowered for what I'm doing. Thus, _get_static_url's will just return
        the hard-coded stuff for now.

    :return: A dictionary of static urls, of the form {'resource_name': 'resource_url'}
    """
    static_urls = dict()

    for resource in resources:
        static_urls[os.path.basename(resource).replace('.', '_').replace('-', '_')] = os.path.join('static', resource)
    for script in scripts:
        static_urls[os.path.basename(script).replace('.', '_').replace('-', '_')] = os.path.join('static', script)
    static_urls['demographics'] = 'static/html/demographics.html'
    static_urls['success'] = 'static/html/success.html'
    static_urls['submit'] = EXTERNAL_QUESTION_SUBMISSION_ENDPOINT
    return static_urls

static_urls = _get_static_urls()


@app.route('/task', methods=['POST', 'GET'])
def task():
    """
    Accepts a request for a task, and then returns the static URLs pointing to all the resources.

    NOTES
        The template variables corresponding to the resources are generally named with their filename (no directory or
        folder information) + their extension.

    :return: The Task / Practice / Error page / etc HTML.
    """
    assignment_id = request.values.get('assignmentId', '')
    is_preview = request.values.get('assignmentId') == PREVIEW_ASSIGN_ID
    hit_id = request.values.get('hitId', '')
    hit_info = mt.get_hit(hit_id)
    # TODO: Get HIT information.
    if not is_preview:
        worker_id = request.values.get('workerId', '')
    else:
        worker_id = None
    task_id = hit_info.RequesterAnnotation
    response = fetch_task(dbget, dbset, task_id, worker_id, is_preview=is_preview, static_urls=static_urls)
    return response


@app.route('/submit', methods=['POST', 'GET'])
def submit():
    """
    Allows a user to submit a task, and inputs all the relevant data into the database.

    :return: Success page.
    """
    worker_ip = request.remote_addr
    hit_id = request.json[0]['hitId']
    hit_info = mt.get_hit(hit_id)
    try:
        hit_type_id = hit_info.HITTypeId
    except AttributeError as e:
        _log.warn('No HIT type ID associated with hit %s' % hit_id)
        hit_type_id = ''
    dbset.task_finished_from_json(request.json, hit_type_id=hit_type_id, worker_ip=worker_ip)
    # TODO: Implement this!
    import ipdb
    ipdb.set_trace()


# make sure the damn thing can use HTTPS
context = ('%s/%s.crt' % (CERT_DIR, CERT_NAME), '%s/%s.key' % (CERT_DIR, CERT_NAME))

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=12344,
            debug=True, ssl_context=context)