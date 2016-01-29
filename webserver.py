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
# from generate import fetch_task
# from generate import make_demographics
from mturk import MTurk
# from daemon import Daemon
import boto.mturk.connection
import happybase
from conf import *
from flask import Flask
from flask import request

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
    hitinfo = mt.get_hit(hit_id)
    # TODO: Get HIT information.
    if not is_preview:
        worker_id = request.values.get('workerId', '')
    import ipdb
    ipdb.set_trace()

# make sure the damn thing can use HTTPS
context = ('%s/%s.crt' % (CERT_DIR, CERT_NAME), '%s/%s.key' % (CERT_DIR, CERT_NAME))

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=12344,
            debug=True, ssl_context=context)
