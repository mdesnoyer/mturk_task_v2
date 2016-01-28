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
import pyramid.testing
from db import Get
from db import Set
from generate import fetch_task
from generate import make_demographics
from mturk import MTurk
from daemon import Daemon
import boto.mturk.connection
import happybase
from conf import *


# <START TESTING>
request = pyramid.testing.DummyRequest()
# <END TESTING>

conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate the database connection.
if MTURK_SANDBOX:
    mturk_host = MTURK_SANDBOX_HOST
else:
    mturk_host = MTURK_HOST
mtconn = boto.mturk.connection.MTurkConnection(host=mturk_host)  # instantiate the mturk connection.

dbget = Get(conn)
dbset = Set(conn)
mturk = MTurk(mtconn)

# _resource_dict is a mapping of static asset template variable names to their locations.
resources = [
    'resources/instr_screenshots/accept_1.jpg',
    'resources/instr_screenshots/accept_2.jpg',
    'resources/instr_screenshots/reject_1.jpg',
    'resources/instr_screenshots/reject_1.jpg',
    'symbols/error.png'
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


def _get_static_urls(request):
    """
    Accepts a request for a task, and then returns the static URLs pointing to all the resources.

    NOTES
        The template variables corresponding to the resources are generally named with their filename (no directory or
        folder information) + their extension.

    :param request: A pyramid request object.
    :return: A dictionary of static urls, of the form {'resource_name': 'resource_url'}
    """
    static_urls = dict()
    for resource in resources:
        static_urls[os.path.basename(resource)] = request.static_url('mturk_app_v2:assets/%s' % resource)
    for script in scripts:
        static_urls[os.path.basename(script)] = request.static_url('mturk_app_v2:scripts/%s' % script)
    return static_urls


def request_for_task(request):
    """
    The task request function. Ingests a request for a new task, and returns the HTML appropriate for the task. If it
    cannot fetch the task, returns the error page.

    NOTES
        This function no longer checks if the participant is banned; this is now managed by the daemon. The information
        is kept on MTurk, so that if they are banned, they can't see the information in the first place.

    :param request: A Pyramid request object.
    :return: The Task / Practice / Error page / etc HTML.
    """
    static_urls = _get_static_urls(request)
    is_preview = request.GET.getone('assignmentId') == PREVIEW_ASSIGN_ID
    worker_id = request.GET.getone('workerId')
    task_id = request.GET.getone('taskId')
    return fetch_task(conn, worker_id, task_id, is_preview=is_preview)


def request_for_demographics(request):
    """
    Returns the HTML for the demographics page.

    :param request: A Pyramid request object.
    :return: The demographics page HTML.
    """
    static_urls = _get_static_urls(request)
    return make_demographics(static_urls=static_urls)


def submission_of_task(request):
    """
    The submission function. Ingests data from a completed task, and updates the database as appropriate. If an error
    occurs, it returns the error page.

    :param request: A Pyramid request object containing the task completion information.
    :return: None.
    """
    # TODO: Implement this!
    # TODO: Remember that if it's not a practice, you have to run decrement_worker_daily_quota in the mturk instance.
    import ipdb
    ipdb.set_trace()
    raise NotImplementedError()


def error(request):
    """
    Creates HTML for a task request error.

    :param request: A pyramid request object.
    :return: None.
    """
    # TODO: Implement this!
    raise NotImplementedError()


if __name__ == '__main__':
    config = Configurator()
    # Register the static asset directory
    # TODO: Find out of add_static_view adds directories recursively...
    config.add_static_view(name='assets', path='mturk_app_v2:resources')
    config.add_static_view(name='scripts', path='mturk_app_v2:js')
    # what is a 'route' and why do they need to be added?
    config.add_route('task', '/task')
    config.add_route('demographics', '/demographics')
    config.add_route('error', '/error')
    config.add_route('submit', '/submit')
    # bind the handler functions to the routes
    config.add_view(request_for_task, route_name='task')
    config.add_view(request_for_demographics, route_name='demographics')
    config.add_view(submission_of_task, rout_name='submit')
    config.add_view(error, route_name='error')
    app = config.make_wsgi_app()
    # 0.0.0.0 means 'listen all all TCP interfaces' -- default is 127.0.0.1
    server = make_server('0.0.0.0', 8080, app)
    server.serve_forever()