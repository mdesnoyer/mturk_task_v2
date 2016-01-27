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

from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response
from db import Get
from db import Set
from generate import fetch_task
from mturk import MTurk
from daemon import Daemon
import boto.mturk.connection
import happybase
from conf import *

conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate the database connection.
if MTURK_SANDBOX:
    mturk_host = MTURK_SANDBOX_HOST
else:
    mturk_host = MTURK_HOST
mtconn = boto.mturk.connection.MTurkConnection(host=mturk_host)  # instantiate the mturk connection.

dbget = Get(conn)
dbset = Set(conn)
mturk = MTurk(mtconn)


def request_for_task(request):
    """
    The task request function. Ingests a request for a new task, and returns the HTML appropriate for the task.

    NOTES
        This function no longer checks if the participant is banned; this is now managed by the daemon. The information
        is kept on MTurk, so that if they are banned, they can't see the information in the first place.

    :param request: A Pyramid request object.
    :return: The Task / Practice / Error page / etc HTML.
    """
    is_preview = request.GET.getone('assignmentId') == PREVIEW_ASSIGN_ID
    worker_id = request.GET.getone('workerId')
    task_id = request.GET.getone('taskId')
    return fetch_task(conn, worker_id, task_id, is_preview=is_preview)


def submission_of_task(request):
    """
    The submission function. Ingests data from a completed task, and updates the database as appropriate.

    :param request: A Pyramid request object containing the task completion information.
    :return: None.
    """
    # TODO: implement
    # TODO: figure out what these requests even look like!
    # TODO: Remember that if it's not a practice, you have to run decrement_worker_daily_quota in the mturk instance.
    raise NotImplementedError()


if __name__ == '__main__':
    # TODO: edit this!
    config = Configurator()
    # what is a 'route' and why do they need to be added?
    config.add_route('hello', '/hello/{name}')
    # adds the view for this route, I think.
    config.add_view(hello_world, route_name='hello')
    app = config.make_wsgi_app()
    # 0.0.0.0 means 'listen all all TCP interfaces' -- default is 127.0.0.1
    server = make_server('0.0.0.0', 8080, app)
    server.serve_forever()