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

import logger
import get as dbget
from dill import dumps, loads
from itertools import combinations as comb
from datetime import datetime
from conf import *
import time


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
    if dbget.table_exists(conn, table):
        if not clobber:
            # it exists and you can't do anything about it.
            return False
        # delete the table
        conn.delete_table(table, disable=True)
    conn.create_table(table, families)
    return dbget.table_exists(conn, table)


def _conv_dict_vals(data):
    """
    Converts a dictionary's values to strings, so they can be stored in HBase.

    :param data: The dictionary you wish to store.
    :return: The converted dictionary.
    """
    for k, v in data.iteritems():
        if v is None:
            data[k] = ''
        elif type(v) is bool:
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


def _get_image_dict(image_url):
    """
    Returns a dictionary for image data, appropriate for inputting into the image table.

    :param image_url: The URL of the image, as a string.
    :return: If the image can be found and opened, a dictionary. Otherwise None.
    """
    width, height = get_im_dims(image_url)
    if width == None:
        return None
    aspect_ratio = '%.3f'%(float(width) / height)
    im_dict = {'metadata:width': width, 'metadata:height': height, 'metadata:aspectRatio': aspect_ratio,
               'metadata:url': image_url, 'metadata:isActive': FALSE}
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


def _get_pair_dict(image1, image2, task_id, attribute):
    """
    Creates a dictionary appropriate for the creation of a pair entry in the Pairs table.

    NOTE:
        In the table, imId1 is always the lexicographically first image.

    :param image1: Image 1 ID.
    :param image2: Image 2 ID.
    :param task_id: The task ID.
    :param attribute: The task attribute.
    :return: A dictionary for use as hbase input.
    """
    if image1 > image2:
        im1 = image2
        im2 = image1
    else:
        im1 = image1
        im2 = image2
    pair_dict = {'metadata:imId1': im1, 'metadata:imId2': im2, 'metadata:task_id': task_id,
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


def create_task_type_table(conn, clobber=False):
    """
    Creates a HIT type table, that stores information about HIT types.

    :param conn: The HappyBase connection object.
    :param clobber: Boolean, if true will erase old HIT type table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    _log.info('Creating HIT type table')
    return _create_table(conn, HIT_TYPE, HIT_TYPE_FAMILIES, clobber)


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
    succ = succ and create_task_type_table(conn, clobber=True)
    return succ


"""
FUNCTIONS FOR LEGACY DATA
"""


def register_legacy_task(conn, task_id, exp_seq):
    """
    Registers a legacy task.

    :param conn: The HappyBase connection object.
    :param task_id: A string, the task ID.
    :param exp_seq: A list of lists, in order of presentation, one for each segment. See Notes in register_task()
    :return: None.
    """
    # TODO: implement
    raise NotImplementedError()


def register_legacy_win(conn, winner_id, loser_id, task_id):
    """
    Registers a legacy win.

    :param conn: The HappyBase connection object.
    :param winner_id: The image_id of the winning image.
    :param loser_id: The image_id of the losing image.
    :param task_id: A string, the task ID.
    :return: None.
    """
    # TODO: implement
    raise NotImplementedError()


"""
ADDING /CHANGING DATA
"""


def register_hit_type(conn, hit_type_id, task_attribute=None, image_attributes=None, title=None, description=None,
                      reward=None, assignment_duration=None, keywords=None, auto_approve_delay=None,
                      is_practice=FALSE, active=FALSE):
    """
    Registers a HIT type in the database.

    :param conn: The HappyBase connection object.
    :param hit_type_id: The HIT type ID, as provided by mturk (see webserver.mturk.register_hit_type_mturk).
    :param task_attribute: The task attribute for tasks that are HITs assigned to this HIT type.
    :param image_attributes: The image attributes for tasks that are HITs assigned to this HIT type.
    :param title: The HIT Type title.
    :param description: The HIT Type description.
    :param reward: The reward for completing this type of HIT.
    :param assignment_duration: How long this HIT Type persists for.
    :param keywords: The HIT type keywords.
    :param auto_approve_delay: The auto-approve delay.
    :param is_practice: FALSE or TRUE (see conf). Whether or not this HIT type should be used for practice tasks
                        (remember that they are mutually exclusive; no hit type should be used for both practice and
                        authentic/'real' tasks.)
    :param active: FALSE or TRUE (see conf). Whether or not this HIT is active, i.e., if new HITs / Tasks should be
                   assigned to this HIT type.
    :return: None.
    """
    _log.info('Registering HIT Type %s' % hit_type_id)
    if active is not FALSE and active is not TRUE:
        _log.warning('Unknown active status, defaulting to FALSE')
        active = FALSE
    if is_practice is not FALSE and is_practice is not TRUE:
        _log.warning('Unknown practice status, defaulting to FALSE')
        is_practice = FALSE
    hit_type_dict = {'metadata:task_attribute': task_attribute, 'metadata:image_attributes': image_attributes,
                     'metadata:title': title, 'metadata:description': description, 'metadata:reward': reward,
                     'metadata:assignment_duration': assignment_duration, 'metadata:keywords': keywords,
                     'metadata:auto_approve_delay': auto_approve_delay, 'metadata:is_practice': is_practice,
                     'status:active': active}
    table = conn.table(HIT_TYPE)
    table.put(hit_type_id, _conv_dict_vals(hit_type_dict))


def register_task(conn, task_id, exp_seq, attribute, blocks=None, is_practice=False, check_ims=False,
                  image_attributes=[]):
    """
    Registers a new task to the database.

    NOTES:
        exp_seq takes the following format:
            [segment1, segment2, ...]
        where segmentN is:
            [type, [image tuples]]
        where type is either keep or reject and tuples are:
            [(image1-1, ..., image1-M), ..., (imageN-1, ..., imageN-M)]

        Since we don't need to check up on these values all that often, we will be converting them to strings using
        dill.

    :param conn: The HappyBase connection object.
    :param task_id: The task ID, as a string.
    :param exp_seq: A list of lists, in order of presentation, one for each segment. See Notes.
    :param attribute: The image attribute this task pertains to, e.g., 'interesting.'
    :param blocks: The experimental blocks (fit for being placed into make_html)
    :param is_practice: A boolean, indicating if this task is a practice or not. [def: False]
    :param check_ims: A boolean. If True, it will check that every image required is in the database.
    :param image_attributes: The set of attributes that the images from this task have.
    :return: None.
    """
    _log.info('Registering task %s' % task_id)
    # determine the total number of images in this task.
    task_dict = dict()
    pair_list = set()  # set of pairs
    task_dict['metadata:is_practice'] = is_practice
    task_dict['metadata:attribute'] = attribute
    images = set()
    image_list = [] # we also need to store the images as a list incase some need to be incremented more than once
    im_tuples = []
    im_tuple_types = []
    table = conn.table(IMAGE_TABLE)
    for seg_type, segment in exp_seq:
        for im_tuple in segment:
            for im in im_tuple:
                if check_ims:
                    if not dbget.image_is_active(conn, im):
                        _log.warning('Image %s is not active or does not exist.' % im)
                        continue
                # TODO: Check that pair does not exist - function should be in get.xxx
                images.add(im)
                image_list.append(im)
            for imPair in comb(im_tuple, 2):
                pair_list.add(tuple(sorted(imPair)))
            im_tuples.append(im_tuple)
            im_tuple_types.append(seg_type)
    for img in image_list:
        table.counter_inc(img, 'stats:num_times_seen')
    task_dict['metadata:images'] = dumps(images)  # note: not in order of presentation!
    task_dict['metadata:tuples'] = dumps(im_tuples)
    task_dict['metadata:tuple_types'] = dumps(im_tuple_types)
    task_dict['metadata:attributes'] = dumps(image_attributes)
    task_dict['status:awaiting_serve'] = TRUE
    task_dict['status:awaiting_hit_group'] = TRUE
    if blocks is None:
        _log.error('No block structure defined for this task - will not be able to load it.')
    else:
        task_dict['blocks:c1'] = dumps(blocks)
    # TODO: Compute forbidden workers?
    # Input the data for the task table
    table = conn.table(TASK_TABLE)
    table.put(task_id, _conv_dict_vals(task_dict))
    # Input the data for the pair table.
    if is_practice and not STORE_PRACTICE_PAIRS:
        return
    table = conn.table(PAIR_TABLE)
    b = table.batch()
    for pair in pair_list:
        pid = _get_pair_key(pair[0], pair[1])
        b.put(pid, _get_pair_dict(pair[0], pair[1], task_id, attribute))
    b.send()


def activate_hit_type(conn, hit_type_id):
    """
    Activates a HIT type, i.e., indicates that it currently has tasks / HITs. being added to it.

    :param conn: The HappyBase connection object.
    :param hit_type_id: The HIT type ID, as provided by mturk.
    :return: None
    """
    # TODO: implement
    raise NotImplementedError()


def deactivate_hit_type(conn, hit_type_id):
    """
    Deactivates a HIT type, so that it is no longer accepting new tasks / HITs.
    :param conn: The HappyBase connection object.
    :param hit_type_id: The HIT type ID, as provided by mturk.
    :return: None
    """
    # TODO: implement
    raise NotImplementedError()


def indicate_task_has_hit_group(conn, task_id):
    """
    Sets status.awaiting_hit_group parameter of the task, indicating that it has been added to a HIT Group

    :param conn: The HappyBase connection object.
    :param task_id: The task ID, as a string.
    :return: None
    """
    table = conn.table(TASK_TABLE)
    table.put(task_id, {'status:awaiting_hit_group': TRUE})


def set_task_html(conn, task_id, html):
    """
    Stores the task HTML in the database, for future reference.

    :param conn: The HappyBase connection object.
    :param task_id: The task ID, as a string.
    :param html: The task HTML, as a string. [this might need to be pickled?]
    :return: None
    """
    table = conn.table(TASK_TABLE)
    table.put(task_id, {'html:c1': html})


def register_worker(conn, worker_id):
    """
    Registers a new worker to the database.

    NOTES:
        This must be keyed by their Turk ID for things to work properly.

    :param conn: The HappyBase connection object.
    :param worker_id: A string, the worker ID.
    :return: None.
    """
    _log.info('Registering worker %s' % worker_id)
    table = conn.table(WORKER_TABLE)
    if dbget.table_has_row(table, worker_id):
        _log.warning('User %s already exists, aborting.' % worker_id)
    table.put(worker_id, {'status:passed_practice': FALSE, 'status:is_legacy': FALSE, 'status:is_banned': FALSE,
                          'status:random_seed': str(int((datetime.now()-datetime(2016, 1, 1)).total_seconds()))})


def register_images(conn, image_ids, image_urls, attributes=[]):
    """
    Registers one or more images to the database.

    :param conn: The HappyBase connection object.
    :param image_ids: A list of strings, the image IDs.
    :param image_urls: A list of strings, the image URLs (in the same order as image_ids).
    :param attributes: The image attributes. This will allow us to run separate experiments on different subsets of the
                       available images. This should be a list of strings, such as "people." They are set to True here.
    :return: None.
    """
    # get the table
    _log.info('Registering %i images.'%(len(image_ids)))
    table = conn.table(IMAGE_TABLE)
    b = table.batch()
    if type(attributes) is str:
        attributes = [attributes]
    for iid, iurl in zip(image_ids, image_urls):
        imdict = _get_image_dict(iurl)
        if imdict is None:
            continue
        for attribute in attributes:
            imdict['attributes:%s' % attribute] = TRUE
            import ipdb
            ipdb.set_trace()
        b.put(iid, imdict)
    b.send()


def add_attributes_to_images(conn, image_ids, attributes):
    """
    Adds attributes to the requested images.

    :param conn: The HappyBase connection object.
    :param image_ids: A list of strings, the image IDs.
    :param attributes: The image attributes, as a list. See register_images
    :return: None
    """
    if type(image_ids) is str:
        image_ids = [image_ids]
    if type(attributes) is str:
        attributes = [attributes]
    _log.info('Adding %i attributes to %i images.'%(len(attributes), len(image_ids)))
    table = conn.table(IMAGE_TABLE)
    b = table.batch()
    for iid in image_ids:
        up_dict = dict()
        for attribute in attributes:
            up_dict['attributes:%s' % attribute] = TRUE
        b.put(iid, up_dict)
    b.send()


def activate_images(conn, image_ids):
    """
    Activates some number of images, i.e., makes them available for tasks.

    :param conn: The HappyBase connection object.
    :param image_ids: A list of strings, the image IDs.
    :return: None.
    """
    _log.info('Activating %i images.'%(len(image_ids)))
    table = conn.table(IMAGE_TABLE)
    b = table.batch()
    for iid in image_ids:
        if not dbget.table_has_row(table, iid):
            _log.warning('No data for image %s'%(iid))
            continue
        b.put(iid, {'metadata:is_active': TRUE})
    b.send()


def activate_n_images(conn, n, image_attributes=IMAGE_ATTRIBUTE):
    """
    Activates N new images.

    :param conn: The HappyBase connection object.
    :param n: The number of new images to activate.
    :param image_attributes: The attributes of the images we are selecting.
    :return: None.
    """
    table = conn.table(IMAGE_TABLE)
    scanner = table.scan(columns=['metadata:is_active'],
                         filter=attribute_image_filter(image_attributes, only_active=True))
    to_activate = []
    for row_key, rowData in scanner:
        to_activate.append(row_key)
        if len(to_activate) == n:
            break
    activate_images(conn, to_activate)


def practice_served(conn, task_id, worker_id):
    """
    Notes that a practice has been served to a worker.

    :param conn: The HappyBase connection object.
    :param task_id: The ID of the practice served.
    :param worker_id: The ID of the worker to whom the practice was served.
    :return: None.
    """
    _log.info('Serving practice %s to worker %s' % task_id, worker_id)
    # TODO: Check that worker exists
    table = conn.table(WORKER_TABLE)
    table.counter_inc(worker_id, 'stats:num_practices_attempted')
    table.counter_inc(worker_id, 'stats:num_practices_attempted_this_week')


def task_served(conn, task_id, worker_id, hit_id=None, hit_type_id=None, payment=None):
    """
    Notes that a task has been served to a worker.

    :param conn: The HappyBase connection object.
    :param task_id: The ID of the task served.
    :param worker_id: The ID of the worker to whom the task was served.
    :param hit_id: The MTurk HIT ID, if known.
    :param hit_type_id: The hash of the task attribute and the image attributes, as produced by
                      webserver.mturk.get_hit_type_id
    :param payment: The task payment, if known.
    :return: None.
    """
    _log.info('Serving task %s served to %s'%(task_id, worker_id))
    table = conn.table(TASK_TABLE)
    table.put(task_id, _conv_dict_vals({'metadata:worker_id': worker_id, 'metadata:hit_id': hit_id,
                                        'metadata:hit_type_id': hit_type_id, 'metadata:payment': payment,
                                        'status:pending_completion': TRUE, 'status:awaiting_serve': FALSE}))
    table = conn.table(WORKER_TABLE)
    # increment the number of incomplete trials for this worker.
    table.counter_inc(worker_id, 'stats:num_incomplete')
    table.counter_inc(worker_id, 'stats:numAttempted')
    table.counter_inc(worker_id, 'stats:numAttemptedThisWeek')


def worker_demographics(conn, worker_id, gender, age, location):
    """
    Sets a worker's demographic information.

    :param conn: The HappyBase connection object.
    :param worker_id: The ID of the worker.
    :param gender: Worker gender.
    :param age: Worker age range.
    :param location: Worker location.
    :return: None
    """
    _log.info('Saving demographics for worker %s'%(worker_id))
    table = conn.table(WORKER_TABLE)
    table.put(worker_id, _conv_dict_vals({'demographics:age': age, 'demographics:gender': gender,
                                          'demographics:location': location}))


def task_finished(conn, task_id, worker_id, choices, choice_idxs, reaction_times, hit_id=None):
    """
    Notes that a user has completed a task.

    :param conn: The HappyBase connection object.
    :param task_id: The ID of the task completed.
    :param worker_id: The ID of the worker (from MTurk)
    :param choices: In-order choice sequence as image IDs (empty if no choice made).
    :param choice_idxs: In-order choice index sequence as integers (empty if no choice made).
    :param reaction_times: In-order reaction times, in msec (empty if no choice made).
    :return: None
    """

    _log.info('Saving complete data for task %s worker %s'%(task_id, worker_id))
    table = conn.table(TASK_TABLE)
    # check that we have task information for this task
    if not dbget.table_has_row(table, task_id):
        # issue a warning, but do not discard the data
        _log.warning('No task data for finished task %s' % task_id)
    # update the data
    table.put(task_id, {'metadata:hit_id': hit_id, 'completed_data:choices': dumps(choices),
                        'completed_data:reaction_times': dumps(reaction_times),
                        'completed_data:choice_idxs': dumps(choice_idxs),
                        'status:pending_completion': FALSE, 'status:pending_evaluation': TRUE})
    database_worker_id = table.row(task_id, columns=['metadata:worker_id']).get('metadata:worker_id', None)
    if database_worker_id != worker_id:
        _log.warning('The task was completed by a different worker than in our database.')
    # TODO: the if checks below are largely deprecated.
    # check that we have worker information for this task
    if worker_id is None:
        # store as much data as you can, but do not attempt to update anything about the worker
        _log.warning('Finished task %s is not associated with a worker.' % task_id)
        return
    if worker_id == '':
        _log.warning('Worker %s attempted to submit task after expiration' % worker_id)
        # TODO: Decide on the behavior here?
        return
    # increment the worker counts
    table = conn.table(WORKER_TABLE)
    table.counter_dec(worker_id, 'stats:num_incomplete')
    table.counter_inc(worker_id, 'stats:num_pending_eval')


def practice_pass(conn, task_id):
    """
    Notes a pass of a practice task.

    :param conn: The HappyBase connection object.
    :param task_id: The ID of the practice to reject.
    :return: None.
    """
    table = conn.table(TASK_TABLE)
    # check if the table exists
    if not dbget.table_has_row(table, task_id):
        _log.warning('No practice data for finished practice %s' % task_id)
        return
    worker_id = table.row(task_id, columns=['task_meta:worker_id']).get('task_meta:worker_id', None)
    # check if there is a worker associated with it
    if worker_id is None:
        _log.error('Passed practice %s is not associated with a worker.' % task_id)
        return
    is_practice = table.row(task_id, columns=['task_meta:is_practice']).get('task_meta:is_practice', False)
    if not is_practice:
        _log.warning('Task %s is not a practice.' % task_id)
        return
    _log.info('Practice passed for worker %s' % worker_id)
    table.put(task_id, {'status:complete': '1'})
    table = conn.table(WORKER_TABLE)
    table.put(worker_id, {'worker_status:passed_practice': '1'})
    # TODO: grant the worker the passed practice qualification from webserver.mturk


def practice_failure(conn, task_id, reason=None):
    """
    Notes a failure of a practice task.

    :param conn: The HappyBase connection object.
    :param task_id: The ID of the practice to reject.
    :param reason: The reason why the practice was rejected. [def: None]
    :return: None.
    """
    _log.info('Nothing needs to be logged for a practice failure at this time.')


def accept_task(conn, task_id):
    """
    Accepts a completed task, updating the worker, task, image, and win tables.

    :param conn: The HappyBase connection object.
    :param task_id: The ID of the task to reject.
    :return: None.
    """
    # update task table, get task data
    _log.info('Task %s has been accepted.' % task_id)
    table = conn.table(TASK_TABLE)
    task_status = dbget.get_task_status(conn, task_id)
    if task_status == DOES_NOT_EXIST:
        _log.error('No such task exists!')
        return
    if task_status != EVALUATION_PENDING:
        _log.warning('Task status indicates it is not ready to be accepted, but proceeding anyway')
    task_data = table.row(task_id)
    table.set(task_id, {'status:pending_evaluation': FALSE, 'status:accepted': TRUE})
    # update worker table
    worker_id = task_data.get('metadata:worker_id', None)
    if worker_id is None:
        _log.warning('No associated worker for task %s' % task_id)
    table = conn.table(WORKER_TABLE)
    table.counter_dec(worker_id, 'stats:num_pending_eval')  # decrement pending evaluation count
    table.counter_inc(worker_id, 'stats:num_accepted')  # increment accepted count
    # update images table
    table = conn.table(IMAGE_TABLE)
    # unfortunately, happybase does not support batch incrementation (arg!)
    choices = loads(task_data.get('completed_data:choices', None))
    for img in choices:
        table.counter_inc(img, 'stats:num_wins')
    # update the win matrix table
    table = conn.table(WIN_TABLE)
    b = table.batch()
    img_tuples = task_data.get('metadata:tuples', None)
    img_tuple_types = task_data.get('metadata:tuple_types', None)
    worker_id = task_data.get('metadata:worker_id', None)
    attribute = task_data.get('metadata:attribute', None)
    # iterate over all the values, and store the data in the win table -- as a batch
    ids_to_inc = []  # this will store all the ids that we have to increment (but which cant be incremented in a batch)
    for ch, tup, tuptype in zip(choices, img_tuples, img_tuple_types):
        # TODO: Account for the situation where no choice is made!
        for img in tup:
            if img != ch:
                if tuptype.lower() == 'keep':
                    # compute the id for this win element
                    cid = ch + ',' + img
                    ids_to_inc.append(cid)
                    b.put(cid, _conv_dict_vals({'data:winner_id': ch, 'data:loser_id': img, 'data:task_id': task_id,
                                                'data:worker_id': worker_id, 'data:attribute': attribute}))
                else:
                    cid = img + ',' + ch
                    ids_to_inc.append(cid)
                    b.put(cid, _conv_dict_vals({'data:winner_id': img, 'data:loser_id': ch, 'data:task_id': task_id,
                                                'data:worker_id': worker_id, 'data:attribute': attribute}))
    b.send()
    for cid in ids_to_inc:
        table.counter_inc(cid, 'data:win_count')  # this increment accounts for legacy shit (uggg)


def reject_task(conn, task_id, reason=None):
    """
    Rejects a completed task.

    :param conn: The HappyBase connection object.
    :param task_id: The ID of the task to reject.
    :param reason: The reason why the task was rejected. [def: None]
    :return: None.
    """
    # fortunately, not much needs to be done for this.
    # update task table, get task data
    _log.info('Task %s has been rejected.' % task_id)
    table = conn.table(TASK_TABLE)
    task_status = dbget.get_task_status(conn, task_id)
    if task_status == DOES_NOT_EXIST:
        _log.error('No such task exists!')
        return
    if task_status != EVALUATION_PENDING:
        _log.warning('Task status indicates it is not ready to be accepted, but proceeding anyway')
    table.set(task_id, _conv_dict_vals({'status:pending_evaluation': FALSE, 'status:rejected': TRUE,
                                        'status:rejection_reason': reason}))
    # update worker table
    task_data = table.row(task_id)
    worker_id = task_data.get('metadata:worker_id', None)
    if worker_id is None:
        _log.warning('No associated worker for task %s' % task_id)
    table = conn.table(WORKER_TABLE)
    table.counter_dec(worker_id, 'stats:num_pending_eval')  # decrement pending evaluation count
    table.counter_inc(worker_id, 'stats:num_rejected')  # increment rejected count
    table.counter_inc(worker_id, 'stats:num_rejected_this_week')


def reset_worker_counts(conn):
    """
    Resets the weekly counters back to 0.

    NOTES:
        The weekly count approach is probably
    :param conn: The HappyBase connection object.
    :return: None.
    """
    table = conn.table(WORKER_TABLE)
    scanner = table.scan(filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
    for row_key, data in scanner:
        table.counter_set(row_key, 'stats:num_practices_attempted_this_week', value=0)
        table.counter_set(row_key, 'stats:num_attempted_this_week', value=0)
        table.counter_set(row_key, 'stats:num_rejected_this_week', value=0)


def worker_autoban_check(conn, worker_id, duration=None):
    """
    Checks that the worker should be autobanned, and bans them if appropriate.

    :param conn: The HappyBase connection object.
    :param worker_id: The worker ID, as a string.
    :return: True if the worker should be autobanned, False otherwise.
    """
    if dbget.worker_weekly_rejected(conn, worker_id) > MIN_REJECT_AUTOBAN_ELIGIBLE:
        if dbget.worker_weekly_reject_accept_ratio(conn, worker_id) > AUTOBAN_REJECT_ACCEPT_RATIO:
            ban_worker(conn, worker_id, reason='You have been rejected too often.')
            return True
    return False


def ban_worker(conn, worker_id, duration=DEFAULT_BAN_LENGTH, reason=DEFAULT_BAN_REASON):
    """
    Bans a worker for some amount of time.

    :param conn: The HappyBase connection object.
    :param worker_id: The worker ID, as a string.
    :param duration: The amount of time to ban the worker for, in seconds [default: 1 week]
    :param reason: The reason for the ban.
    :return: None.
    """
    table = conn.table(WORKER_TABLE)
    table.put(worker_id, _conv_dict_vals({'status:is_banned': TRUE, 'status:ban_duration': duration,
                                          'status:ban_reason': reason}))


def worker_ban_expires_in(conn, worker_id):
    """
    Checks whether or not a worker's ban has expired; if so, it changes the ban status and returns 0. Otherwise, it
    returns the amount of time left in the ban.

    :param conn: The HappyBase connection object.
    :param worker_id: The worker ID, as a string.
    :return: 0 if the subject is not or is no longer banned, otherwise returns the time until the ban expires.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(worker_id, include_timestamp=True)
    ban_data = data.get('status:is_banned', (FALSE, 0))
    if ban_data[0] == FALSE:
        return 0
    ban_date = time.mktime(time.localtime(float(ban_data[1])/1000))
    cur_date = time.mktime(time.localtime())
    ban_dur = float(data.get('status:ban_length', ('0', 0))[0])
    if (cur_date - ban_date) > ban_dur:
        table.set(worker_id, {'status:is_banned': FALSE, 'status:ban_length': '0'})
        return 0
    else:
        return (cur_date - ban_date) - ban_dur


def reset_timed_out_tasks(conn):
    """
    Checks if a task has been pending for too long without completion; if so, it resets it.

    :param conn: The HappyBase connection object.
    :return: None
    """
    table = conn.table(TASK_TABLE)
    to_reset = []  # a list of task IDs to reset.
    scanner = table.scan(columns=['status:pending_completion'], filter=PENDING_COMPLETION_FILTER, include_timestamp=True)
    for row_key, rowData in scanner:
        start_timestamp = rowData.get('status:pending_completion', (FALSE, '0'))[1]
        start_date = time.mktime(time.localtime(float(start_timestamp)/1000))
        cur_date = time.mktime(time.localtime())
        if (cur_date - start_date) > TASK_COMPLETION_TIMEOUT:
            to_reset.append(row_key)
    # Now, un-serve all those tasks
    b = table.batch()
    for task_id in to_reset:
        b.put(task_id, _conv_dict_vals({'metadata:worker_id': '', 'metadata:assignment_id': '',
                                        'metadata:hit_id': '', 'metadata:payment': '',
                                        'status:pending_completion': FALSE, 'status:awaiting_serve': TRUE}))
    b.send()
    _log.info('Found %i incomplete tasks to be reset.' % len(to_reset))