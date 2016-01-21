"""
This script actually runs the webserver. There are only two endpoints:
    request     - requests a task from the server
    submit      - submits a completed task to the server
"""

from wsgiref.simple_server import make_server
from pyramid.config import Configurator
from pyramid.response import Response
from mturk_task_v2.database import get as dbget
from mturk_task_v2.database import set as dbset
from mturk_task_v2.generate import request_for_task
import happybase
from conf import *

conn = happybase.Connection(DATABASE_LOCATION)  # make sure to instantiate the database connection

def request_for_task(request):
    """
    The task request function. Ingests a request, and returns the HTML appropriate for the task.

    :param request: A Pyramid request object.
    :return: The Task / Practice / Error page / etc HTML.
    """
    is_preview = request.GET.getone('assignmentId') == PREVIEW_ASSIGN_ID
    workerId = request.GET.getone('workerId')
    taskId = request.GET.getone('taskId')
    return request_for_task.fetch_task(conn, workerId, taskId)


def submission_of_task(request):
    raise NotImplementedError()


if __name__ == '__main__':
    config = Configurator()
    config.add_route('hello', '/hello/{name}')                  # what is a 'route' and why do they need to be added?
    config.add_view(hello_world, route_name='hello')            # adds the view for this route, I think.
    app = config.make_wsgi_app()
    server = make_server('0.0.0.0', 8080, app)                  # 0.0.0.0 means 'listen all all TCP interfaces' -- default is 127.0.0.1
    server.serve_forever()