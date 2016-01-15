"""
Handles general query events for the task. These are more abstract than the 'set' functions, as there is a larger
variety of possibilities here.
"""

from conf import *
import numpy as np
from itertools import combinations as comb
import logger
import set as dbset
from dill import loads
import time

#  LOGGING ##############################

_log = logger.setup_logger(__name__)

# /LOGGING ##############################


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


def _get_preexisting_pairs(conn, images):
    """
    Returns all pairs that have already occurred among a list of images.

    NOTE:
        There is an open question about to the approach here, with respect to which one is more efficient:
            (1) Iterate over each pair in images, and see if they exist.
            (2) Select all pairs for each image with a prefix and look for pairs where the other image is also in
            images.
        For now, we will use (1), for its conceptual similarity.

    :param images: A iterable of image IDs
    :param conn: The HappyBase connection object.
    :return: A set of tuples of forbidden image ID pairs.
    """
    found_pairs = set()
    table = conn.table(PAIR_TABLE)
    for im1, im2 in comb(images, 2):
        pairId = _get_pair_key(im1, im2)
        if _pair_exists(conn, pairId):
            found_pairs.add(_pair_to_tuple(im1, im2))
    return found_pairs


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
    # TODO: Edit this so that it corresponds with the answer from math.stackexchange
    return lambda time_seen: base_prob + (1. - base_prob) * (2 ** (time_seen - min_seen))


def _pair_exists(conn, pair_key):
    """
    Returns True if a pair already exists in the database.

    :param conn: The HappyBase connection object.
    :param pair_key: The key to search for the pair with.
    :return: True if pair exists, False otherwise.
    """
    table = conn.table(PAIR_TABLE)
    return table_has_row(table, pair_key)


def _tuple_permitted(im_tuple, ex_pairs, conn=None):
    """
    Returns True if the tuple is allowable.

    Note:
        If both table and conn are omitted, then the search is only performed over the extant (i.e., in-memory) pairs.

    :param im_tuple: The candidate tuple.
    :param ex_pairs: The pairs already made in this task. (in the form of pair_to_tuple)
    :param conn: The HappyBase connection object.
    :return: True if this tuple may be added, False otherwise.
    """
    # First, check to make sure none of them are already in *this* task, which is much cheaper than the database.
    for im1, im2 in comb(im_tuple, 2):
        pair = _pair_to_tuple(im1, im2)
        if pair in ex_pairs:
            return False
    if conn is None:
        # TODO: Make this warn people only once!
        # _log.info('No connection information provided, search is only performed among in-memory pairs.')
        return True  # The database cannot be checked, and so we will be only looking at the in-memory pairs.
    table = conn.table(PAIR_TABLE)
    for im1, im2 in comb(im_tuple, 2):
        pair_key = _get_pair_key(im1, im2)
        if _pair_exists(conn, pair_key):
            return False
    return True


def _timestamp_to_datetime(timestamp):
    """
    Converts an HBase timestamp (msec since UNIX epoch) to a datestr.

    :param timestamp: The HappyBase (i.e., HBase) timestamp, as a string.
    :return: A datestr.
    """
    raise NotImplementedError()


def _task_has_blockdata(conn, taskId):
    """
    Checks whether or not a task has defined block data.

    NOTES:
        This does not seem possible without fetching all the data! :-(

    :param conn: The HappyBase connection object.
    :param taskId: The Task ID, as a string.
    :return: True if the task has pickled block data associated with it.
    """
    # TODO: Find out if this really is impossible.
    raise NotImplementedError()



# WORKER INFO


def worker_exists(conn, workerId):
    """
    Indicates whether we have a record of this worker in the database.

    :param conn: The HappyBase connection object.
    :param workerId: The Worker ID (from MTurk), as a string.
    :return: True if the there are records of this worker, otherwise false.
    """
    table = conn.table(WORKER_TABLE)
    return table_has_row(table, workerId)


def worker_need_demographics(conn, workerId):
    """
    Indicates whether or not the worker needs demographic information.

    :param conn: The HappyBase connection object.
    :param workerId: The Worker ID (from MTurk), as a string.
    :return: True if we need demographic information from the worker. False otherwise.
    """
    table = conn.table(WORKER_TABLE)
    rowData = table.row(workerId)
    if len(rowData.get('demographics:age', '')):
        return True
    else:
        return False


def worker_need_practice(conn, workerId):
    """
    Indicates whether the worker should be served a practice or a real task.

    :param conn: The HappyBase connection object.
    :param workerId: The Worker ID (from MTurk), as a string.
    :return: True if the worker has not passed a practice yet and must be served one. False otherwise.
    """
    table = conn.table(WORKER_TABLE)
    rowData = table.row(workerId)
    return rowData.get('status:passedPractice', FALSE) == TRUE


def current_worker_practices_number(conn, workerId):
    """
    Returns which practice the worker needs (as 0 ... N)

    :param conn: The HappyBase connection object.
    :param workerId: The Worker ID (from MTurk) as a string.
    :return: An integer corresponding to the practice the worker needs (starting from the top 'row')
    """
    table = conn.table(WORKER_TABLE)
    rowData = table.row(workerId)
    return int(rowData.get('status:numPracticesAttemptedThisWeek', '0'))


def worker_is_banned(conn, workerId):
    """
    Determines whether or not the worker is banned.

    :param conn: The HappyBase connection object.
    :param workerId: The Worker ID (from MTurk), as a string.
    :return: True if the worker is banned, False otherwise.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(workerId, columns=['status:isBanned'])
    return data.get('status:isBanned', FALSE) == TRUE


def worker_attempted_this_week(conn, workerId):
    """
    Returns the number of tasks this worker has attempted this week.

    :param conn: The HappyBase connection object.
    :param workerId: The worker ID, as a string.
    :return: Integer, the number of tasks the worker has attempted this week.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(workerId, columns=['stats:numAttemptedThisWeek'])
    return int(data.get('stats:numAttemptedThisWeek', '0'))


def worker_attempted_too_much(conn, workerId):
    """
    Returns True if the worker has attempted too many tasks.

    :param conn: The HappyBase connection object.
    :param workerId: The worker ID, as a string.
    :return: True if the worker has attempted too many tasks, otherwise False.
    """
    return worker_attempted_this_week(conn, workerId) > MAX_ATTEMPTS_PER_WEEK


def worker_weekly_rejected(conn, workerId):
    """
    Returns the rejection-to-acceptance ratio for this worker for this week.

    :param conn: The HappyBase connection object.
    :param workerId: The worker ID, as a string.
    :return: Float, the number of tasks rejected divided by the number of tasks accepted.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(workerId, columns=['stats:numRejectedThisWeek'])
    return int(data.get('stats:numRejectedThisWeek', '0'))


def worker_weekly_reject_accept_ratio(conn, workerId):
    """
    Returns the rejection-to-acceptance ratio for this worker for this week.

    :param conn: The HappyBase connection object.
    :param workerId: The worker ID, as a string.
    :return: Float, the number of tasks rejected divided by the number of tasks accepted.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(workerId, columns=['stats:numAcceptedThisWeek', 'stats:numRejectedThisWeek'])
    num_acc = float(data.get('stats:numAcceptedThisWeek', '0'))
    num_rej = float(data.get('stats:numRejectedThisWeek', '0'))
    return num_rej / num_acc


# TASK


def get_task_status(conn, taskId):
    """
    Fetches the status code given a task ID.

    :param conn: The HappyBase connection object.
    :param taskId: The task ID, which is the row key.
    :return: A status code, as defined in conf.
    """
    table = conn.table(TASK_TABLE)
    if not table_has_row(table, taskId):
        return DOES_NOT_EXIST
    task = table.row(taskId)
    if task.get('metadata:isPractice', FALSE) == TRUE:
        return IS_PRACTICE
    if task.get('status:awaitingServe', FALSE) == TRUE:
        return AWAITING_SERVE
    if task.get('status:pendingCompletion', FALSE) == TRUE:
        return COMPLETION_PENDING
    if task.get('status:pendingEvaluation', FALSE) == TRUE:
        return EVALUATION_PENDING
    if task.get('status:accepted', FALSE) == TRUE:
        return ACCEPTED
    if task.get('status:rejected', FALSE) == TRUE:
        return REJECTED
    return UNKNOWN_STATUS


def get_available_task(conn, practice=False, practice_n=0):
    """
    Returns an available task.

    :param conn: The HappyBase connection object.
    :param practice: Whether or not to fetch a practice task. [optional]
    :param practice_n: Which practice to serve, starting from 0. [optional]
    :return: The task ID for an available task. If there is no task available, it returns None.
    """
    table = conn.table(TASK_TABLE)
    if practice:
        scanner = table.scan(columns=['metadata:isPractice'],
                             filter=IS_PRACTICE_FILTER)
        cnt = 0
        for taskId, data in scanner:
            if cnt == practice_n:
                # ideally, it'd check if there is block information for this task, but that requires loading the entire
                # object into memory (unless there's some kind of filter for it)
                return taskId
            cnt += 1
        return None
    else:
        try:
            taskId, data = table.scan(columns=['status:awaitingServe'],
                                      filter=AWAITING_SERVE_FILTER).next()
        except StopIteration:
            # TODO: regenerate more tasks in this case.
            _log.warning('No tasks available.')
        return taskId


def get_task_blocks(conn, taskId):
    """
    Returns the task blocks, as a list of dictionaries, appropriate for make_html.

    :param conn: The HappyBase connection object.
    :param taskId: The task ID, as a string.
    :return: List of blocks represented as dictionaries, if there is a problem returns None.
    """
    table = conn.table(TASK_TABLE)
    pickled_blocks = table.row(taskId, columns=['blocks:c1']).get('blocks:c1', None)
    if pickled_blocks is None:
        return None
    return loads(pickled_blocks)


# GENERAL QUERIES


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


# IMAGE STUFF


def get_n_active_images(conn):
    """
    Gets a count of active images.

    :param conn: The HappyBase connection object.
    :return: An integer, the number of active images.
    """
    table = conn.table(IMAGE_TABLE)
    # TODO: Find out why the filterIfColumnMissing flag doesnt work! In principle, it's always added...but still...
    # note: do NOTE use binary prefix, because 1 does not correspond to the string 1, but to the binary 1.
    scanner = table.scan(columns=['metadata:isActive'], filter=ACTIVE_FILTER)
    active_image_count = 0
    for item in scanner:
        active_image_count += 1
    return active_image_count


def image_is_active(conn, imageId):
    """
    Returns True if an image has been registered into the database and is an active image.

    :param conn: The HappyBase connection object.
    :param imageId: The image ID, which is the row key.
    :return: True if the image is active. False otherwise.
    """
    table = conn.table(IMAGE_TABLE)
    isActive = table.row(imageId, columns=['metadata:isActive']).get('metadata:isActive', None)
    if isActive == TRUE:
        return True
    else:
        return False


def image_get_min_seen(conn):
    """
    Returns the number of times the least-seen image has been seen. (I.e., the number of tasks it has been featured in.

    NOTES: This only applies to active images.

    :param conn: The HappyBase connection object.
    :return: Integer, the min number of times seen.
    """
    obs_min = np.inf
    table = conn.table(IMAGE_TABLE)
    # note that if we provide a column argument, rows without this column are not emitted.
    scanner = table.scan(columns=['stats:numTimesSeen'], filter=ACTIVE_FILTER)
    been_seen = 0
    for row_key, row_data in scanner:
        been_seen += 1
        cur_seen = row_data.get('stats:numTimesSeen', 0)
        if cur_seen < obs_min:
            obs_min = cur_seen
        if obs_min == 0:
            return 0  # it can't go below 0, so you can cut the scan short
    if not been_seen:
        return 0 # this is an edge case, where none of the images have been seen.
    return obs_min


def get_n_images(conn, n, base_prob=None):
    """
    Returns n images from the database, sampled according to some probability. These are fit for use in design
    generation.

    NOTES:
        If not given, the base_prob is defined to be n / N, where N is the number of active images.

    :param conn: The HappyBase connection object.
    :param n: Number of images to choose.
    :param base_prob: The base probability of selecting any image.
    :return: A list of image IDs.
    """
    n_active = get_n_active_images(conn)
    if n > n_active:
        _log.warning('Insufficient number of active images, activating %i more.' % ACTIVATION_CHUNK_SIZE)
        dbset.activate_n_images(conn, ACTIVATION_CHUNK_SIZE)
    min_seen = image_get_min_seen(conn)
    if min_seen > SAMPLES_REQ_PER_IMAGE(n_active):
        _log.warning('Images are sufficiently sampled, activating %i images.' % ACTIVATION_CHUNK_SIZE)
        dbset.activate_n_images(conn, ACTIVATION_CHUNK_SIZE)
    if base_prob is None:
        base_prob = float(n) / n_active
    p = _prob_select(base_prob, min_seen)
    table = conn.table(IMAGE_TABLE)
    images = set()
    while len(images) < n:
        # repeatedly scan the database, selecting images -- don't bother selecting the numTimesSeen column, since it
        # wont be defined for images that have yet to be seen.
        scanner = table.scan(filter=ACTIVE_FILTER)
        for row_key, row_data in scanner:
            cur_seen = row_data.get('stats:numTimesSeen', 0)
            if np.random.rand() < p(cur_seen):
                images.add(row_key)
    return list(images)


# TASK DESIGN STUFF


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
    :return: A list of tuples representing each subset. Elements may be randomized within trial and subset order may
    be randomized without consequence.
    """
    occ = np.zeros(n)  # an array which stores the number of times an image has been used.
    design = []
    images = get_n_images(conn, n, 0.05)
    np.random.shuffle(images)  # shuffle the images (remember its in-place! >.<)
    obs = _get_preexisting_pairs(conn, images)  # the set of observed tuples
    for allvio in range(t):  # minimize the number of j-violations (i.e., elements appearing more than j-times)
        for c in comb(range(n), t):
            if np.min(occ) == j:
                return design  # you're done
            cvio = 0  # the count of current violations
            # check the candidate tuple
            cur_tuple = tuple([images[x] for x in c])
            if not _tuple_permitted(cur_tuple, obs):
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


def get_task(conn, n, t, j, n_keep_blocks=None, n_reject_blocks=None, prompt=None, practice=False,
             attribute=ATTRIBUTE, random_segment_order=RANDOMIZE_SEGMENT_ORDER):
    """
    Creates a new task, by calling get_design and then arranging those tuples into keep and reject blocks, and then
    registering it in the database.

    NOTE:
        Keep blocks always come first, after which they alternate between Keep / Reject. If the RANDOMIZE_SEGMENT_ORDER
        option is true, then the segments order will be randomized.

        The randomization has to be imposed here, along with all other order decisions, because the database assumes
        that data from mechanical turk (i.e., as determined by the task HTML) are in the same order as the data in the
        database.

    :param conn: The HappyBase connection object.
    :param n: The number of distinct elements involved in the experiment.
    :param t: The number of elements to present each trial.
    :param j: The number of times each element should appear during the experiment.
    :param n_keep_blocks: The number of keep blocks in this task (tuples are evenly divided among them)
    :param n_reject_blocks: The number of reject blocks in this task (tuples are evenly divided among them)
    :param prompt: The prompt to use across all blocks (overrides defaults)
    :param: practice: Boolean, whether or not this task is a practice.
    :param: attribute: The task attribute.
    :param: random_seqment_order: Whether or not to randomize block ordering.
    :return: None.
    """
    if practice:
        taskId = practice_id_gen()
    else:
        taskId = task_id_gen()
    if n_keep_blocks is None:
        if practice:
            n_keep_blocks = DEF_PRACTICE_KEEP_BLOCKS
        else:
            n_keep_blocks = DEF_KEEP_BLOCKS
    if n_reject_blocks is None:
        if practice:
            n_reject_blocks = DEF_PRACTICE_REJECT_BLOCKS
        else:
            n_reject_blocks = DEF_REJECT_BLOCKS
    if prompt is None:
        if practice:
            prompt = DEF_PRACTICE_PROMPT
        else:
            prompt = DEF_PROMPT
    # get the tuples
    image_tuples = get_design(conn, n, t, j)
    # arrange them into blocks
    keep_tuples = [image_tuples[i:i+n] for i in xrange(0, len(l), n_keep_blocks)]
    reject_tuples = [image_tuples[i:i+n] for i in xrange(0, len(l), n_reject_blocks)]
    keep_blocks = []
    reject_blocks = []
    for kt in keep_tuples:
        block = dict()
        block['images'] = [list(x) for x in kt]
        block['type'] = KEEP_BLOCK
        block['instructions'] = DEF_KEEP_BLOCK_INSTRUCTIONS
        block['prompt'] = prompt
        keep_blocks.append(block)
    for kt in reject_tuples:
        block = dict()
        block['images'] = [list(x) for x in kt]
        block['type'] = REJECT_BLOCK
        block['instructions'] = DEF_REJECT_BLOCK_INSTRUCTIONS
        block['prompt'] = prompt
        reject_blocks.append(block)
    blocks = []
    while (not len(keep_blocks)) and (not len(reject_blocks)):
        if len(keep_blocks):
            blocks.append(keep_blocks.pop(0))
        if len(reject_blocks):
            blocks.append(reject_blocks.pop(0))
    if random_segment_order:
        np.random.shuffle(blocks)
    # define expSeq
    # annoying expSeq expects image tuples...
    expSeq = [[x['type'], [typle(y) for y in x['images']]] for x in blocks]
    dbset.register_task(conn, taskId, expSeq, attribute, blocks=blocks, isPractice=practice, checkIms=True)



