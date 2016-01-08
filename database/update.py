"""
Handles all update events for the database. The following events are possible, which loosely fall into 6 groups:

    Group I
    New task to be registered.
    New worker to be registered.
    New image(s) to be registered.

    Group II
    A practice has been served to a worker.
    A task has been served to a worker.

    Group III
    A worker's demographic information has to be logged.

    Group IV
    A worker has finished a task.
    A worker has passed a practice.
    A worker has failed a practice.

    Group V
    A task has been accepted.
    A task has been rejected.

    Group VI
    Image(s) to be activated.
"""

from conf import *


def register_task(conn, taskId, filename, expSeq, isPractice=False, checkIms=False):
    """
    Registers a new task to the database.

    :param conn: The HBase connection object.
    :param taskId: A string, the task ID.
    :param filename: A string, the filename holding the task.
    :param expSeq: A list of lists, in order of presentation, one for each segment. See Notes.
    :param isPractice: A boolean, indicating if this task is a practice or not. [def: False]
    :param checkIms: A boolean. If True, it will check that every image required is in the database.
    :return: True if the registration completed successfully. False otherwise.

    NOTES:
        expSeq takes the following format:
            [segment1, segment2, ...]
        where segmentN is:
            [type, [image tuples]]
        where type is either keep or reject and tuples are:
            [(image1-1, ..., image1-M), ..., (imageN-1, ..., imageN-M)]
    """


def register_worker(conn, workerId):
    """
    Registers a new worker to the database.

    :param conn: The HBase connection object.
    :param workerId: A string, the worker ID.
    :return: True if the registration completed successfully. False otherwise.
    """


def register_images(conn, imageIds, imageUrls):
    """
    Registers one or more images to the database.

    :param conn: The HBase connection object.
    :param imageIds: A list of strings, the image IDs.
    :param imageUrls: A list of strings, the image URLs (in the same order as imageIds).
    :return: True if the registration completed successfully. False otherwise.
    """


def practice_served(conn, taskId, workerId):
    """
    Notes that a practice has been served to a worker.

    :param conn: The HBase connection object.
    :param taskId: The ID of the practice served.
    :param workerId: The ID of the worker to whom the practice was served.
    :return: True if successful. False otherwise.
    """


def task_served(conn, taskId, workerId):
    """
    Notes that a task has been served to a worker.

    :param conn: The HBase connection object.
    :param taskId: The ID of the task served.
    :param workerId: The ID of the worker to whom the task was served.
    :return: True if successful. False otherwise.
    """


def worker_demographics(conn, workerId, gender, age, location):
    """
    Sets a worker's demographic information.

    :param conn: The HBase connection object.
    :param workerId: The ID of the worker.
    :param gender: Worker gender.
    :param age: Worker age range.
    :param location: Worker location.
    :return: True if successful. False otherwise.
    """


def task_finished(conn, taskId):
    """
    Notes that a user has completed a task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the task completed.
    :return: True if successful. False otherwise.
    """


def practice_pass(conn, taskId):
    """
    Notes a pass of a practice task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the practice to reject.
    :return: True if successful. False otherwise.
    """


def practice_failure(conn, taskId, reason=None):
    """
    Notes a failure of a practice task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the practice to reject.
    :param reason: The reason why the practice was rejected. [def: None]
    :return: True if successful. False otherwise.
    """


def accept_task(conn, taskId):
    """
    Accepts a completed task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the task to reject.
    :return: True if successful. False otherwise.
    """


def reject_task(conn, taskId, reason=None):
    """
    Rejects a completed task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the task to reject.
    :param reason: The reason why the task was rejected. [def: None]
    :return: True if successful. False otherwise.
    """


def activate_image(conn, imageIds):
    """
    Activates some number of images, i.e., makes them available for tasks.

    :param conn: The HBase connection object.
    :param imageIds: A list of strings, the image IDs.
    :return: True if successful. False otherwise.
    """
