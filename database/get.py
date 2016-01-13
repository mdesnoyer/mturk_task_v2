"""
Handles general query events for the task. These are more abstract than the 'set' functions, as there is a larger
variety of possibilities here.
"""

from conf import *
import numpy as np
from itertools import combinations as comb


def _pair_to_tuple(image1, image2):
    """
    Converts an image pair into a sorted tuple.

    :param image1: An image ID (ordering irrelevant)
    :param image2: An image ID (ordering irrelevant)
    :return: A sorted image tuple.
    """
    if image1 > image2:
        return (image2, image1)
    else:
        return (image1, image2)


def _get_pair_key(image1, image2):
    """
    Returns a row key for a given pair. Row keys for pairs are the image IDs, separated by a comma, and sorted
    alphabetically. The inputs need not be sorted alphabetically.

    :param image1: Image 1 ID.
    :param image2: Image 2 ID.
    :return: The pair row key, as a string.
    """
    return ','.join(_pair_to_tuple(image1, image2))


def table_exists(conn, tableName):
    """
    Checks if a table exists.

    :param conn: The HappyBase connection object.
    :param tableName: The name of the table to check for existence.
    :return: True if table exists, false otherwise.
    """
    return tableName in conn.tables()


def table_has_row(table, rowKey):
    """
    Determines if a table has a defined row key or not.

    :param table: A HappyBase table object.
    :param rowKey: The desired row key, as a string.
    :return: True if key exists, false otherwise.
    """
    scan = table.scan(row_start=rowKey, filter='KeyOnlyFilter() AND FirstKeyOnlyFilter()', limit=1)
    return next(scan, None) is not None


def image_is_active(imageId, table=None, conn=None):
    """
    Returns True if an image has been registered into the database and is an active image.

    :param imageId: The image ID, which is the row key.
    :param table: The HappyBase table object for images (if provided, conn may be omitted).
    :param conn: The HappyBase connection object (if provided, table may be omitted).
    :return: True if the image is active. False otherwise.
    """
    if table is None:
        if conn is None:
            raise ValueError('No connection information provided.')
        table = conn.table(IMAGE_TABLE)
    isActive = table.row(imageId, columns=['metadata:isActive']).get('metadata:isActive', None)
    if isActive == TRUE:
        return True
    else:
        return False


def get_task_status(taskId, table=None, conn=None):
    """
    Fetches the status code given a task ID.

    :param taskId: The task ID, which is the row key.
    :param table: The HappyBase table object for tasks (if provided, conn may be omitted).
    :param conn: The HappyBase connection object (if provided, table may be omitted).
    :return: A status code, as defined in conf.
    """
    if table is None:
        if conn is None:
            raise ValueError('No connection information provided.')
        table = conn.table(TASK_TABLE)
    if not table_has_row(table, taskId):
        return DOES_NOT_EXIST
    task = table.row(taskId)
    if task.get('metadata:isPractice', FALSE) == TRUE:
        return IS_PRACTICE
    if task.get('status:pendingCompletion', FALSE) == TRUE:
        return COMPLETION_PENDING
    if task.get('status:pendingEvaluation', FALSE) == TRUE:
        return EVALUATION_PENDING
    if task.get('status:accepted', FALSE) == TRUE:
        return ACCEPTED
    if task.get('status:rejected', FALSE) == TRUE:
        return REJECTED
    return UNKNOWN_STATUS


def get_num_items(table):
    """
    Counts the number of rows in a table.

    NOTES: This is likely to be pretty inefficient.

    :param table: A HappyBase table object.
    :return: An integer, the number of rows in the object.
    """
    x = table.scan(filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
    tot_ims = 0
    for key, d in x:
        tot_ims += 1
    return tot_ims


def get_items(table):
    """
    Gets all the items represented in a table.

    :param table: A HappyBase table object.
    :return: The items in the table.
    """
    scanner = table.scan(filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
    keys = []
    for key, d in scanner:
        keys.append(key)
    return keys


def image_get_min_seen(conn):
    """
    Returns the number of times the least-seen image has been seen. (I.e., the number of tasks it has been featured in.

    NOTES: This only applies to active images.

    :param conn: The HappyBase connection object.
    :return: Integer, the min number of times seen.
    """
    obs_min = np.inf
    table = conn.table(IMAGE_TABLE)
    # note that this filter is potentially dangerous, since it accepts all rows where metdata:isActive starts with '1'.
    active_filter = "SingleColumnValueFilter ('metadata','isActive',=,'binaryprefix:^%s$')" % TRUE
    scanner = table.scan(columns=['stats:numTimesSeen'], filter=active_filter)
    for row_key, row_data in scanner:
        cur_seen = row_data.get('stats:numTimesSeen', 0)
        if cur_seen < obs_min:
            obs_min = cur_seen
        if obs_min == 0:
            return 0  # it can't go below 0, so you can cut the scan short
    return obs_min


def _prob_select(base_prob, min_seen):
    """
    Returns a lambda function giving the probability of an image being selected for a new task.

    NOTES:
        The function is given by:
            BaseProb + (1 - BaseProb) * 2 ^ -(times_seen - min_seen)

        As a private function, it assumes the input is correct and does not check it.

    :param base_prob: Base probability of selection
    :param min_seen: The global min for the number of times an image has been in a task.
    :return: A lambda function that accepts a single parameter (times_seen) and returns the probability of selecting
    this image.
    """
    return lambda x: base_prob + (1. - base_prob) * (2 ** (times_seen - min_seen))


def _pair_exists(pair_key, table=None, conn=None):
    """
    Returns True if a pair already exists in the database.

    :param pair_key: The key to search for the pair with.
    :param table: The HappyBase table object for pairs (if provided, conn may be omitted).
    :param conn: The HappyBase connection object (if provided, table may be omitted).
    :return: True if pair exists, False otherwise.
    """
    if table is None:
        if conn is None:
            raise ValueError('No connection information provided.')
        table = conn.table(PAIR_TABLE)
    return table_has_row(table, pair_key)


def _tuple_permitted(im_tuple, ex_pairs, table=None, conn=None):
    """
    Returns True if the tuple is allowable.

    :param im_tuple: The candidate tuple.
    :param ex_pairs: The pairs already made in this task.
    :param table: The HappyBase table object for pairs (if provided, conn may be omitted).
    :param conn: The HappyBase connection object (if provided, table may be omitted).
    :return: True if this tuple may be added, False otherwise.
    """
    # First, check to make sure none of them are already in *this* task, which is much cheaper than the database.
    for im1, im2 in comb(im_tuple, 2):
        pair = _pair_to_tuple(im1, im2)
        if pair in ex_pairs:
            return False
    if table is None:
        if conn is None:
            raise ValueError('No connection information provided.')
        table = conn.table(PAIR_TABLE)
    for im1, im2 in comb(im_tuple, 2):
        pair_key = _get_pair_key(im1, im2)
        if _pair_exists(pair_key, table=table):
            return False
    return True


def get_n_images(conn, n, base_prob):
    """
    Returns n images from the database.

    NOTES:
        If the number of images requested is larger than the number of images in the database, this function will not
        terminate.

    :param conn: The HappyBase connection object.
    :param n: Number of images to choose.
    :param base_prob: The base probability of selecting any image.
    :return: A list of image IDs.
    """
    min_seen = image_get_min_seen(conn)
    p = _prob_select(base_prob, min_seen)
    table = conn.table(IMAGE_TABLE)
    images = set()
    active_filter = "SingleColumnValueFilter ('metadata','isActive',=,'binaryprefix:^%s$')" % TRUE
    while len(images) < n:
        # repeatedly scan the database, selecting images
        scanner = table.scan(columns=['stats:numTimesSeen'], filter=active_filter)
        for row_key, row_data in scanner:
            cur_seen = row_data.get('stats:numTimesSeen', 0)
            if np.random.rand() < p(cur_seen):
                images.add(row_key)
    return images


def get_design(conn, n, t, j):
    """
    Returns a task design, as a series of tuples of images. This is based directly on generate/utils/get_design, which
    should be consulted for reference on the creation of Steiner systems.

    This extends get_design by not only checking against oc-occurance within the task, but also globally across all
    tasks by invoking _tuple_permitted.

    :param conn: The HappyBase connection object.
    :param n: The number of distinct elements involved in the experiment.
    :param t: The number of elements to present each trial.
    :param j: The number of times each element should appear during the experiment.
    :return: A list of tuples representing each subset. Elements may be randomized within element and subset order may
    be randomized without consequence.
    """
    obs = set()  # the set of observed tuples
    occ = np.zeros(n)  # an array which stores the number of times an image has been used.
    design = []
    images = get_n_images(conn, n, 0.05)
    for allvio in range(t):  # minimize the number of j-violations (i.e., elements appearing more than j-times)
        for c in comb(range(n), t):
            if np.min(occ) == j:
                return design  # you're done
            cvio = 0  # the count of current violations
            # check the candidate tuple
            cur_tuple = (images[x] for x in c)
            if not _tuple_permitted(cur_tuple, obs, conn=conn):
                continue
            for i in c:
                cvio += max(0, occ[i] - j + 1)
            if cvio > allvio:
                continue
            for x1, x2 in comb(c, 2):
                obs.add(_pair_to_tuple(x1, x2))
            for i in c:
                occ[i] += 1
            design.append(cur_tuple)
    if not np.min(occ) >= j:
        return None
    return design
