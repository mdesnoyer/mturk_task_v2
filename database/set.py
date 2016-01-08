"""
Handles all update events for the database. The following events are possible, which loosely fall into groups:

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

    Group VII
    Add legacy worker - done via register_worker
    Add legacy task
    Add legacy pair - done via register_pair
    Add legacy win

    Group VIII
    Create/recreate workers table
    Create/recreate tasks table
    Create/recreate images table
    Create/recreate pairs table
    Create/recreate wins table
"""

from conf import *
import urllib, cStringIO
from PIL import Image
import happybase as hb
from .. import logger
from get import table_exists, table_has_row

#  LOGGING ##############################

_log = logger.setup_logger(__name__)

# /LOGGING ##############################


def _create_table(conn, table, families, clobber):
    """
    General create table function.

    :param conn: The HBase connection object.
    :param table: The table name.
    :param families: The table families, see conf.py
    :param clobber: Boolean, if true will erase old workers table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    if table_exists(conn, table):
        if not clobber:
            # it exists and you can't do anything about it.
            return False
        # delete the table
        conn.delete_table(table, disable=True)
    conn.create_table(table, families)
    return table_exists(conn, table)


def _get_im_dims(imageUrl):
    """
    Returns the dimensions of an image file in pixels as [width, height]. This is unfortunately somewhat time
    consuming as the images have to be loaded in order to determine their dimensions. Its likely that this can be
    accomplished in pure javascript, however I (a) don't know enough javascript and (b) want more explicit control.

    :param image: The filename or URL of an image.
    :return: A list, the dimensions of the image in pixels, as (width, height).
    """
    try:
        file = cStringIO.StringIO(urllib.urlopen(imageUrl).read())
    except IOError:
        _log.error('Could not fetch image at: %s' % imageUrl)
        return None, None
    try:
        im = Image.open(file)
    except:
        _log.error('Could not convert image to PIL at: %s' % imageUrl)
        return None, None
    width, height = im.size
    return width, height


def _conv_dict_vals(data):
    """
    Converts a dictionary's values to strings, so they can be stored in HBase.

    :param data: The dictionary you wish to store.
    :return: The converted dictionary.
    """
    for k, v in data.iteritems():
        data[k] = str(v)
    return data


def _create_arbitrary_dict(data, prefix):
    """
    Converts a list of items into a numbered dict, in which the keys are the item indices.

    :param data: A list of items.
    :return: A dictionary that can be stored in HBase
    """
    return {prefix + ':%i' % (n): v for n, v in enumerate(data)}


def _get_image_dict(imageUrl):
    """
    Returns a dictionary for image data, appropriate for inputting into the image table.
    :param imageUrl: The URL of the image, as a string.
    :return: If the image can be found and opened, a dictionary. Otherwise None.
    """
    width, height = _get_im_dims(imageUrl)
    if width == None:
        return None
    aspect_ratio = '%.3f'%(float(width) / height)
    im_dict = {'image_meta:width': width, 'image_meta:height': height, 'image_meta:aspectRatio': aspect_ratio,
               'image_meta:url': imageUrl, 'image_meta:isActive': 0}
    return _conv_dict_vals(im_dict)


def create_worker_table(conn, clobber=False):
    """
    Creates a workers table, with names based on conf.

    :param conn: The HBase connection object.
    :param clobber: Boolean, if true will erase old workers table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating worker table.')
    return _create_table(conn, WORKER_TABLE, WORKER_FAMILIES, clobber)


def create_task_table(conn, clobber=False):
    """
    Creates a tasks table, with names based on conf.

    :param conn: The HBase connection object.
    :param clobber: Boolean, if true will erase old tasks table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating task table.')
    return _create_table(conn, TASK_TABLE, TASK_FAMILIES, clobber)


def create_image_table(conn, clobber=False):
    """
    Creates a images table, with names based on conf.

    :param conn: The HBase connection object.
    :param clobber: Boolean, if true will erase old images table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating image table.')
    return _create_table(conn, IMAGE_TABLE, IMAGE_FAMILIES, clobber)


def create_pair_table(conn, clobber=False):
    """
    Creates a pairs table, with names based on conf.

    :param conn: The HBase connection object.
    :param clobber: Boolean, if true will erase old pairs table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating pair table.')
    return _create_table(conn, PAIR_TABLE, PAIR_FAMILIES, clobber)


def create_win_table(conn, clobber=False):
    """
    Creates a wins table, with names based on conf.

    :param conn: The HBase connection object.
    :param clobber: Boolean, if true will erase old wins table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating win table.')
    return _create_table(conn, WIN_TABLE, WIN_FAMILIES, clobber)


def force_regen_tables(conn):
    """
    Forcibly rebuilds all tables.

    WARNING: DO NOT USE THIS LIGHTLY!

    :param conn: The HBase connection object.
    :return: True if the tables were all successfully regenerated.
    """
    succ = True
    succ = succ and create_worker_table(conn, clobber=True)
    succ = succ and create_image_table(conn, clobber=True)
    succ = succ and create_pair_table(conn, clobber=True)
    succ = succ and create_task_table(conn, clobber=True)
    succ = succ and create_win_table(conn, clobber=True)
    return succ


def register_legacy_task(conn, taskId, expSeq):
    """
    Registers a legacy task.

    :param conn: The HBase connection object.
    :param taskId: A string, the task ID.
    :param expSeq: A list of lists, in order of presentation, one for each segment. See Notes in register_task()
    :return: None.
    """


def register_legacy_win(conn, winnerId, loserId, taskId):
    """
    Registers a legacy win.

    :param conn: The HBase connection object.
    :param winnerId: The imageId of the winning image.
    :param loserId: The imageId of the losing image.
    :param taskId: A string, the task ID.
    :return: None.
    """


def register_task(conn, taskId, filename, expSeq, attribute, isPractice=False, checkIms=False):
    """
    Registers a new task to the database.

    NOTES:
        expSeq takes the following format:
            [segment1, segment2, ...]
        where segmentN is:
            [type, [image tuples]]
        where type is either keep or reject and tuples are:
            [(image1-1, ..., image1-M), ..., (imageN-1, ..., imageN-M)]

    :param conn: The HBase connection object.
    :param taskId: A string, the task ID.
    :param filename: A string, the filename holding the task.
    :param expSeq: A list of lists, in order of presentation, one for each segment. See Notes.
    :param attribute: The image attribute this task pertains to, e.g., 'interesting.'
    :param isPractice: A boolean, indicating if this task is a practice or not. [def: False]
    :param checkIms: A boolean. If True, it will check that every image required is in the database.
    :return: None.
    """


def register_worker(conn, workerId):
    """
    Registers a new worker to the database.

    :param conn: The HBase connection object.
    :param workerId: A string, the worker ID.
    :return: None.
    """


def register_images(conn, imageIds, imageUrls):
    """
    Registers one or more images to the database.

    :param conn: The HBase connection object.
    :param imageIds: A list of strings, the image IDs.
    :param imageUrls: A list of strings, the image URLs (in the same order as imageIds).
    :return: None.
    """
    # get the table
    _log.info('Registering %i images.'%(len(imageIds)))
    table = conn.table(IMAGE_TABLE)
    b = table.batch()
    for iid, iurl in zip(imageIds, imageUrls):
        b.put(iid, _get_image_dict(iurl))
    b.send()


def activate_images(conn, imageIds):
    """
    Activates some number of images, i.e., makes them available for tasks.

    :param conn: The HBase connection object.
    :param imageIds: A list of strings, the image IDs.
    :return: None.
    """
    _log.info('Activating %i images.'%(len(imageIds)))
    table = conn.table(IMAGE_TABLE)
    b = table.batch()
    for iid in imageIds:
        if not table_has_row(table, iid):
            _log.warning('No data for image %s'%(iid))
            continue
        b.put(iid, {'image_meta:isActive': '1'})
    b.send()


def practice_served(conn, taskId, workerId):
    """
    Notes that a practice has been served to a worker.

    :param conn: The HBase connection object.
    :param taskId: The ID of the practice served.
    :param workerId: The ID of the worker to whom the practice was served.
    :return: None.
    """
    # TODO: Check that worker exists
    table = conn.table(WORKER_TABLE)
    table.counter_inc(workerId, 'worker_status:numPracticeAttempts')
    task_served(conn, taskId, workerId)


def task_served(conn, taskId, workerId):
    """
    Notes that a task has been served to a worker.

    :param conn: The HBase connection object.
    :param taskId: The ID of the task served.
    :param workerId: The ID of the worker to whom the task was served.
    :return: None.
    """
    _log.info('Task or practice %s served to %s'%(taskId, workerId))
    table = conn.table(TASK_TABLE)
    table.put(taskId, _conv_dict_vals({'task_meta:workerId': workerId, 'status:pendingCompletion': '1'}))
    table = conn.table(WORKER_TABLE)
    # increment the number of incomplete trials for this worker.
    table.counter_inc(workerId, 'worker_task_stats:incomplete')
    table.counter_inc(workerId, 'worker_task_stats:tasksAttempted')

def worker_demographics(conn, workerId, gender, age, location):
    """
    Sets a worker's demographic information.

    :param conn: The HBase connection object.
    :param workerId: The ID of the worker.
    :param gender: Worker gender.
    :param age: Worker age range.
    :param location: Worker location.
    :return: None
    """
    _log.info('Saving demographics for worker %s'%(workerId))
    table = conn.table(WORKER_TABLE)
    table.put(workerId, _conv_dict_vals({'demographics:age': age, 'demographics:gender': gender,
                                         'demographics:location': location}))


def task_finished(conn, taskId, choices, choiceIdxs, reactionTimes):
    """
    Notes that a user has completed a task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the task completed.
    :param choices: In-order choice sequence as image IDs (empty if no choice made).
    :param choiceIdxs: In-order choice index sequence as integers (empty if no choice made).
    :param reactionTimes: In-order reaction times, in msec (empty if no choice made).
    :return: None
    """
    _log.info('Saving complete data for task %s worker %s'%(taskId, workerId))
    table = conn.table(TASK_TABLE)
    # check that we have task information for this task
    if not table_has_row(table, taskId):
        # issue a warning, but do not discard the data
        _log.warning('No task data for finished task %s' % taskId)
    # TODO: Consider doing this as a batch?
    # update the data
    table.put(taskId, _create_arbitrary_dict(choices, prefix='choices'))
    table.put(taskId, _create_arbitrary_dict(choiceIdxs, prefix='choice_idxs'))
    table.put(taskId, _create_arbitrary_dict(reactionTimes, prefix='reaction_times'))
    table.put(taskId, {'status:pendingCompletion':'0', 'status:pendingValidation':'1'})
    workerId = table.row(taskId, columns=['task_meta:workerId']).get('task_meta:workerId', None)
    # check that we have worker information for this task
    if workerId is None:
        # store as much data as you can, but do not attempt to update anything about the worker
        _log.warning('Finished task %s is not associated with a worker.' % taskId)
        return
    # increment the worker counts
    # TODO: Check that the worker has an incomplete task?
    table = conn.table(WORKER_TABLE)
    table.counter_dec(workerId, 'worker_task_stats:incomplete')
    table.counter_inc(workerId, 'worker_task_stats:pendingEvaluation')

def practice_pass(conn, taskId):
    """
    Notes a pass of a practice task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the practice to reject.
    :return: None.
    """
    table = conn.table(TASK_TABLE)
    # check if the table exists
    if not table_has_row(table, taskId):
        _log.warning('No practice data for finished practice %s' % taskId)
        return
    workerId = table.row(taskId, columns=['task_meta:workerId']).get('task_meta:workerId', None)
    # check if there is a worker associated with it
    if workerId is None:
        _log.error('Passed practice %s is not associated with a worker.' % taskId)
        return
    isPractice = table.row(taskId, columns=['task_meta:isPractice'].get('task_meta:isPractice', False))
    if not isPractice:
        _log.warning('Task %s is not a practice.' % taskId)
        return
    _log.info('Practice passed for worker %s' % workerId)
    table.put(taskId, {'status:complete': '1'})
    table = conn.table(WORKER_TABLE)
    table.put(workerId, {'worker_status:passedPractice': '1'})

def practice_failure(conn, taskId, reason=None):
    """
    Notes a failure of a practice task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the practice to reject.
    :param reason: The reason why the practice was rejected. [def: None]
    :return: None.
    """


def accept_task(conn, taskId):
    """
    Accepts a completed task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the task to reject.
    :return: None.
    """


def reject_task(conn, taskId, reason=None):
    """
    Rejects a completed task.

    :param conn: The HBase connection object.
    :param taskID: The ID of the task to reject.
    :param reason: The reason why the task was rejected. [def: None]
    :return: None.
    """