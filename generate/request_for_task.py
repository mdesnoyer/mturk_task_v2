"""
Function is called when an AJAX request carrying a worker ID hits the webserver. It determines what needs to be shown.
"""
from ..conf import *
from ..database import get as dbget
from ..database import set as dbset
import make_html

def build_task(conn, workerId):
    """
    Constructs a task after a request hits the webserver, requiring only the workerId.

    :param conn: The HappyBase connection object.
    :param workerId: The workerId, as a string.
    :return: A filename that points to the HTML for the requested task.
    """
    # check that the worker exists, else register them. We want to have their information in the database so we don't
    # spawn errors down the road.
    if not dbget.worker_exists(conn, workerId):
        dbset.register_worker(conn, workerId)
    # check if the worker is banned
    if dbget.worker_is_banned(conn, workerId):
        if dbset.worker_expire_ban(conn, workerId):
            # TODO: display ban page
            return None
    # check if the worker has completed too many tasks for this week
    if dbget.worker_attempted_too_much(conn, workerId):
        # TODO: display too many trials attempted page
        return None
    # TODO: check if we've paid out too much this week already?
    # check if we need demographics or not
    # check if we need to give the worker a practice
    is_practice = False
    practice_n = None
    collect_demo = False
    if dbget.worker_need_practice(conn, workerId):
        is_practice = True
        practice_n = dbget.current_worker_practices_number(conn, workerId)
        if dbget.worker_need_demographics(conn, workerId):
            collect_demo = True
    taskId = dbget.get_available_task(conn, practice=is_practice, practice_n=practice_n)
    # indicate that the tasks have been served
    if taskId is None:
        # TODO display no tasks available page, maybe with a try-again-later?
        return None
    if is_practice:
        dbset.practice_served(conn, taskId, workerId)
    else:
        dbset.task_served(conn, taskId, workerId)
    blocks = dbget.get_task_blocks(conn, taskId)
    if blocks is None:
        # TODO: display an error-fetching-task page.
        return None
    html = make_html.make(blocks, practice=is_practice, collect_demo=collect_demo)
    # TODO: add the html to the database
    dbset.set_task_html(conn, taskId, html)
    return html