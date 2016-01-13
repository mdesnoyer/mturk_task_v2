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
import logger
from get import table_exists, table_has_row, image_is_active
from dill import dumps, loads
from itertools import combinations as comb
from datetime import datetime

#  LOGGING ##############################

_log = logger.setup_logger(__name__)

# /LOGGING ##############################


"""
PRIVATE FUNCTIONS
"""


def _create_table(conn, table, families, clobber):
    """
    General create table function.

    :param conn: The HappyBase connection object.
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
        if type(v) is bool:
            if v:
                data[k] = TRUE
            else:
                data[k] = FALSE
        else:
            try:
                data[k] = FLOAT_STR % v
            except TypeError:
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
    im_dict = {'metadata:width': width, 'metadata:height': height, 'metadata:aspectRatio': aspect_ratio,
               'metadata:url': imageUrl, 'metadata:isActive': FALSE}
    return _conv_dict_vals(im_dict)


def _get_pair_key(image1, image2):
    """
    Returns a row key for a given pair. Row keys for pairs are the image IDs, separated by a comma, and sorted
    alphabetically. The inputs need not be sorted alphabetically.

    :param image1: Image 1 ID.
    :param image2: Image 2 ID.
    :return: The pair row key, as a string.
    """
    if image1 > image2:
        return str(image2) + ',' + str(image1)
    else:
        return str(image1) + ',' + str(image2)


def _get_pair_dict(image1, image2, taskId, attribute):
    """
    Creates a dictionary appropriate for the creation of a pair entry in the Pairs table.

    :param image1: Image 1 ID.
    :param image2: Image 2 ID.
    :param taskId: The task ID.
    :param attribute: The task attribute.
    :return: A dictionary for use as hbase input.
    """
    if image1 > image2:
        im1 = image2
        im2 = image1
    else:
        im1 = image1
        im2 = image2
    pair_dict = {'metadata:imId1': im1, 'metadata:imId2': im2, 'metadata:taskId': taskId,
                 'metadata:attribute': attribute}
    return _conv_dict_vals(pair_dict)


"""
TABLE CREATION
"""


def create_worker_table(conn, clobber=False):
    """
    Creates a workers table, with names based on conf.

    :param conn: The HappyBase connection object.
    :param clobber: Boolean, if true will erase old workers table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating worker table.')
    return _create_table(conn, WORKER_TABLE, WORKER_FAMILIES, clobber)


def create_task_table(conn, clobber=False):
    """
    Creates a tasks table, with names based on conf.

    :param conn: The HappyBase connection object.
    :param clobber: Boolean, if true will erase old tasks table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating task table.')
    return _create_table(conn, TASK_TABLE, TASK_FAMILIES, clobber)


def create_image_table(conn, clobber=False):
    """
    Creates a images table, with names based on conf.

    :param conn: The HappyBase connection object.
    :param clobber: Boolean, if true will erase old images table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating image table.')
    return _create_table(conn, IMAGE_TABLE, IMAGE_FAMILIES, clobber)


def create_pair_table(conn, clobber=False):
    """
    Creates a pairs table, with names based on conf.

    :param conn: The HappyBase connection object.
    :param clobber: Boolean, if true will erase old pairs table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating pair table.')
    return _create_table(conn, PAIR_TABLE, PAIR_FAMILIES, clobber)


def create_win_table(conn, clobber=False):
    """
    Creates a wins table, with names based on conf.

    :param conn: The HappyBase connection object.
    :param clobber: Boolean, if true will erase old wins table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating win table.')
    return _create_table(conn, WIN_TABLE, WIN_FAMILIES, clobber)


def force_regen_tables(conn):
    """
    Forcibly rebuilds all tables.

    WARNING: DO NOT USE THIS LIGHTLY!

    :param conn: The HappyBase connection object.
    :return: True if the tables were all successfully regenerated.
    """
    succ = True
    succ = succ and create_worker_table(conn, clobber=True)
    succ = succ and create_image_table(conn, clobber=True)
    succ = succ and create_pair_table(conn, clobber=True)
    succ = succ and create_task_table(conn, clobber=True)
    succ = succ and create_win_table(conn, clobber=True)
    return succ


"""
FUNCTIONS FOR LEGACY DATA
"""


def register_legacy_task(conn, taskId, expSeq):
    """
    Registers a legacy task.

    :param conn: The HappyBase connection object.
    :param taskId: A string, the task ID.
    :param expSeq: A list of lists, in order of presentation, one for each segment. See Notes in register_task()
    :return: None.
    """


def register_legacy_win(conn, winnerId, loserId, taskId):
    """
    Registers a legacy win.

    :param conn: The HappyBase connection object.
    :param winnerId: The imageId of the winning image.
    :param loserId: The imageId of the losing image.
    :param taskId: A string, the task ID.
    :return: None.
    """


"""
ADDING DATA
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

        Since we don't need to check up on these values all that often, we will be converting them to strings using
        dill.

    :param conn: The HappyBase connection object.
    :param taskId: A string, the task ID.
    :param filename: A string, the filename holding the task.
    :param expSeq: A list of lists, in order of presentation, one for each segment. See Notes.
    :param attribute: The image attribute this task pertains to, e.g., 'interesting.'
    :param isPractice: A boolean, indicating if this task is a practice or not. [def: False]
    :param checkIms: A boolean. If True, it will check that every image required is in the database.
    :return: None.
    """
    _log.info('Registering task %s' % taskId)
    # determine the total number of images in this task.
    task_dict = dict()
    pair_list = set()  # set of pairs
    task_dict['metadata:isPractice'] = isPractice
    task_dict['metadata:filename'] = filename
    task_dict['metadata:attribute'] = attribute
    images = set()
    im_tuples = []
    im_tuple_types = []
    table = conn.table(IMAGE_TABLE)
    for seg_type, segment in expSeq:
        for im_tuple in segment:
            for im in im_tuple:
                if checkIms:
                    if not image_is_active(im, table=table):
                        _log.warning('Image %s is not active or does not exist.' % im)
                        continue
                # TODO: Check that pair does not exist - function should be in get.xxx
                images.add(im)
            for imPair in comb(im_tuple, 2):
                pair_list.add(tuple(sorted(imPair)))
            im_tuples.append(im_tuple)
            im_tuple_types.append(seg_type)
    for img in images:
        table.counter_inc(img, 'stats:numTimesSeen')
    task_dict['metadata:images'] = dumps(images)  # note: not in order of presentation!
    task_dict['metadata:tuples'] = dumps(im_tuples)
    task_dict['metadata:tupleTypes'] = dumps(im_tuple_types)
    # TODO: Compute forbidden workers?
    # Input the data for the task table
    table = conn.table(TASK_TABLE)
    table.put(taskId, _conv_dict_vals(task_dict))
    # Input the data for the pair table.
    if isPractice and not STORE_PRACTICE_PAIRS:
        return
    table = conn.table(PAIR_TABLE)
    b = table.batch()
    for pair in pair_list:
        pid = _get_pair_key(pair[0], pair[1])
        b.put(pid, _get_pair_dict(pair[0], pair[1], taskId, attribute))
    b.send()


def register_worker(conn, workerId):
    """
    Registers a new worker to the database.

    NOTES:
        This must be keyed by their Turk ID for things to work properly.

    :param conn: The HappyBase connection object.
    :param workerId: A string, the worker ID.
    :return: None.
    """
    _log.info('Registering worker %s' % workerId)
    table = conn.table(WORKER_TABLE)
    if table_has_row(table, workerId):
        _log.warning('User %s already exists, aborting.' % workerId)
    table.put(workerId, {'status:passedPractice': FALSE, 'status:isLegacy': FALSE, 'status:isBanned': FALSE,
                         'status:randomSeed': str(int((datetime.now()-datetime(2016, 1, 1)).total_seconds()))})


def register_images(conn, imageIds, imageUrls):
    """
    Registers one or more images to the database.

    :param conn: The HappyBase connection object.
    :param imageIds: A list of strings, the image IDs.
    :param imageUrls: A list of strings, the image URLs (in the same order as imageIds).
    :return: None.
    """
    # get the table
    _log.info('Registering %i images.'%(len(imageIds)))
    table = conn.table(IMAGE_TABLE)
    b = table.batch()
    for iid, iurl in zip(imageIds, imageUrls):
        imdict = _get_image_dict(iurl)
        if imdict is None:
            continue
        b.put(iid, imdict)
    b.send()


def activate_images(conn, imageIds):
    """
    Activates some number of images, i.e., makes them available for tasks.

    :param conn: The HappyBase connection object.
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
        b.put(iid, {'metadata:isActive': TRUE})
    b.send()


def practice_served(conn, taskId, workerId):
    """
    Notes that a practice has been served to a worker.

    :param conn: The HappyBase connection object.
    :param taskId: The ID of the practice served.
    :param workerId: The ID of the worker to whom the practice was served.
    :return: None.
    """
    _log.info('Serving practice %s to worker %s' % taskId, workerId)
    # TODO: Check that worker exists
    table = conn.table(WORKER_TABLE)
    table.counter_inc(workerId, 'stats:numPracticeAttempts')


def task_served(conn, taskId, workerId, assignmentId=None, hitId=None, payment=None):
    """
    Notes that a task has been served to a worker.

    :param conn: The HappyBase connection object.
    :param taskId: The ID of the task served.
    :param workerId: The ID of the worker to whom the task was served.
    :return: None.
    """
    _log.info('Serving task %s served to %s'%(taskId, workerId))
    table = conn.table(TASK_TABLE)
    table.put(taskId, _conv_dict_vals({'metadata:workerId': workerId, 'metadata:assignmentId': assignmentId,
                                       'metadata:hitId': hitId, 'metadata:payment': payment,
                                       'status:pendingCompletion': TRUE}))
    table = conn.table(WORKER_TABLE)
    # increment the number of incomplete trials for this worker.
    table.counter_inc(workerId, 'stats:numIncomplete')
    table.counter_inc(workerId, 'stats:numAttempted')


def worker_demographics(conn, workerId, gender, age, location):
    """
    Sets a worker's demographic information.

    :param conn: The HappyBase connection object.
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

    :param conn: The HappyBase connection object.
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
    # update the data
    table.put(taskId, {'completed_data:choices': dumps(choices),
                       'completed_data:reactionTimes': dumps(reactionTimes),
                       'completed_data:choiceIdxs': dumps(choiceIdxs),
                       'status:pendingCompletion': FALSE, 'status:pendingEvaluation': TRUE})
    workerId = table.row(taskId, columns=['metadata:workerId']).get('metadata:workerId', None)
    # check that we have worker information for this task
    if workerId is None:
        # store as much data as you can, but do not attempt to update anything about the worker
        _log.warning('Finished task %s is not associated with a worker.' % taskId)
        return
    # increment the worker counts
    table = conn.table(WORKER_TABLE)
    table.counter_dec(workerId, 'stats:numIncomplete')
    table.counter_inc(workerId, 'stats:numPendingEval')


def practice_pass(conn, taskId):
    """
    Notes a pass of a practice task.

    :param conn: The HappyBase connection object.
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

    :param conn: The HappyBase connection object.
    :param taskID: The ID of the practice to reject.
    :param reason: The reason why the practice was rejected. [def: None]
    :return: None.
    """
    _log.info('Nothing needs to be logged for a practice failure at this time.')

def accept_task(conn, taskId):
    """
    Accepts a completed task, updating the worker, task, image, and win tables.

    :param conn: The HappyBase connection object.
    :param taskID: The ID of the task to reject.
    :return: None.
    """
    # update task table, get task data
    _log.info('Task %s has been accepted.' % taskId)
    table = conn.table(TASK_TABLE)
    task_status = get_task_status(taskId, table=table)
    if task_status == DOES_NOT_EXIST:
        _log.error('No such task exists!')
        return
    if task_status != EVALUATION_PENDING:
        _log.warning('Task status indicates it is not ready to be accepted, but proceeding anyway')
    task_data = table.row(taskId)
    table.set(taskId, {'status:pendingEvaluation': FALSE, 'status:accepted': TRUE})
    # update worker table
    table = conn.table(WORKER_TABLE)
    table.counter_dec(workerId, 'stats:numPendingEval')  # decrement pending evaluation count
    table.counter_inc(workerId, 'stats:numAccepted')  # increment accepted count
    # update images table
    table = conn.table(IMAGE_TABLE)
    all_images = loads(task_data.get('metadata:images', None))
    # unfortunately, happybase does not support batch incrementation (arg!)
    choices = loads(task_data.get('completed_data:choices', None))
    for img in choices:
        table.counter_inc(img, 'stats:numWins')
    # update the win matrix table
    table = conn.table(WIN_TABLE)
    b = table.batch()
    img_tuples = task_data.get('metadata:tuples', None)
    img_tuple_types = task_data.get('metadata:tupleTypes', None)
    workerId = task_data.get('metadata:workerId', None)
    attribute = task_data.get('metadata:attribute', None)
    # iterate over all the values, and store the data in the win table -- as a batch
    ids_to_inc = []  # this will store all the ids that we have to increment (but which cant be incremented in a batch)
    for ch, tup, tuptype in zip(choices, img_tuples, img_tuple_types):
        for img in tup:
            if img != ch:
                if tuptype.lower() == 'keep':
                    # compute the id for this win element
                    cid = ch + ',' + img
                    ids_to_inc.append(cid)
                    b.put(cid, _conv_dict_vals({'data:winnerId': ch, 'data:loserId': img, 'data:taskId': taskId,
                                                'data:workerId': workerId, 'data:attribute': attribute}))
                else:
                    cid = img + ',' + ch
                    ids_to_inc.append(cid)
                    b.put(cid, _conv_dict_vals({'data:winnerId': img, 'data:loserId': ch, 'data:taskId': taskId,
                                           'data:workerId': workerId, 'data:attribute': attribute}))
    b.send()
    for cid in ids_to_inc:
        table.counter_inc(cid, 'data:winCount') # this increment accounts for legacy shit (uggg)


def reject_task(conn, taskId, reason=None):
    """
    Rejects a completed task.

    :param conn: The HappyBase connection object.
    :param taskID: The ID of the task to reject.
    :param reason: The reason why the task was rejected. [def: None]
    :return: None.
    """
    # fortunately, not much needs to be done for this.
    # update task table, get task data
    _log.info('Task %s has been rejected.' % taskId)
    table = conn.table(TASK_TABLE)
    task_status = get_task_status(taskId, table=table)
    if task_status == DOES_NOT_EXIST:
        _log.error('No such task exists!')
        return
    if task_status != EVALUATION_PENDING:
        _log.warning('Task status indicates it is not ready to be accepted, but proceeding anyway')
    task_data = table.row(taskId)
    table.set(taskId, _conv_dict_vals({'status:pendingEvaluation': FALSE, 'status:rejected': TRUE,
                                       'status:rejectionReason': reason}))
    # update worker table
    table = conn.table(WORKER_TABLE)
    table.counter_dec(workerId, 'stats:numPendingEval')  # decrement pending evaluation count
    table.counter_inc(workerId, 'stats:numRejected')  # increment accepted count