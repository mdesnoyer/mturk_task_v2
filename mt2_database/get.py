"""
Handles general query events for the task. These are more abstract than the 'set' functions, as there is a larger
variety of possibilities here.
"""

from itertools import combinations as comb
from .. import logger
import set as dbset
from dill import loads
from dill import dumps
from conf import *
import time

#  LOGGING ##############################

_log = logger.setup_logger(__name__)

# /LOGGING ##############################


def _get_pair_key(image1, image2):
    """
    Returns a row key for a given pair. Row keys for pairs are the image IDs, separated by a comma, and sorted
    alphabetically. The inputs need not be sorted alphabetically.

    :param image1: Image 1 ID.
    :param image2: Image 2 ID.
    :return: The pair row key, as a string.
    """
    return ','.join(pair_to_tuple(image1, image2))


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
            found_pairs.add(pair_to_tuple(im1, im2))
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
        pair = pair_to_tuple(im1, im2)
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


def _timestamp_to_struct_time(timestamp):
    """
    Converts an HBase timestamp (msec since UNIX epoch) to a struct_time.

    :param timestamp: The HappyBase (i.e., HBase) timestamp, as an int.
    :return: A struct_time object.
    """
    return time.localtime(float(timestamp)/1000)


def _get_ban_expiration_date_str(ban_issued, ban_length):
    """
    Returns the date a ban expires.

    :param ban_issued: The timestamp the ban was issued on, an int, in HBase / HappyBase format (msec since epoch)
    :param ban_length: The duration of the ban, an int, seconds.
    :return: The date the ban expires, as a string.
    """
    expire_time = ban_issue + ban_length
    return _get_timedelta_string(expire_time, time.mktime(time.localtime()))


def _get_timedelta_string(timestamp1, timestamp2):
    """
    Returns a string, time from now, when the ban expires.

    :param timestamp1: First timestamp, as an int in HBase / Happybase style (msec since epoch)
    :param timestamp2: Second timestamp, as an int in HBase / Happybase style (msec since epoch)
    :return: The timedelta, in weeks, days, hours, minutes and seconds, as a string.
    """
    week_len = 7 * 24 * 60 * 60.
    day_len = 24 * 60 * 60.
    hour_len = 60 * 60.
    min_len = 60.
    time_delta = abs(float(timestamp1)/1000 - float(timestamp2)/1000)
    weeks, secs = divmod(time_delta, week_len)
    days, secs = divmod(secs, day_len)
    hours, secs = divmod(secs, hour_len)
    minutes, secs = divmod(secs, min_len)
    time_list = [[weeks, 'week'], [days, 'day'], [hours, 'hour'], [minutes, 'minute'], [secs, 'second']]

    def get_name(num, noun):
        if not num:
            return ''
        if num == 1:
            return '%i %s' % (num, noun)
        else:
            return '%i %ss' % (num, noun)
    cur_strs = []
    for num, noun in time_list:
        cur_strs.append(get_name(num, noun))
    return ', '.join(filter(lambda x: len(x), cur_strs))


def _task_has_blockdata(conn, task_id):
    """
    Checks whether or not a task has defined block data.

    NOTES:
        This does not seem possible without fetching all the data! :-(

    :param conn: The HappyBase connection object.
    :param task_id: The Task ID, as a string.
    :return: True if the task has pickled block data associated with it.
    """
    # TODO: Find out if this really is impossible.
    raise NotImplementedError()


# WORKER INFO


def worker_exists(conn, worker_id):
    """
    Indicates whether we have a record of this worker in the database.

    :param conn: The HappyBase connection object.
    :param worker_id: The Worker ID (from MTurk), as a string.
    :return: True if the there are records of this worker, otherwise false.
    """
    table = conn.table(WORKER_TABLE)
    return table_has_row(table, worker_id)


def worker_need_demographics(conn, worker_id):
    """
    Indicates whether or not the worker needs demographic information.

    :param conn: The HappyBase connection object.
    :param worker_id: The Worker ID (from MTurk), as a string.
    :return: True if we need demographic information from the worker. False otherwise.
    """
    table = conn.table(WORKER_TABLE)
    rowData = table.row(worker_id)
    if len(rowData.get('demographics:age', '')):
        return True
    else:
        return False


def worker_need_practice(conn, worker_id):
    """
    Indicates whether the worker should be served a practice or a real task.

    :param conn: The HappyBase connection object.
    :param worker_id: The Worker ID (from MTurk), as a string.
    :return: True if the worker has not passed a practice yet and must be served one. False otherwise.
    """
    table = conn.table(WORKER_TABLE)
    rowData = table.row(worker_id)
    return rowData.get('status:passed_practice', FALSE) != TRUE


def current_worker_practices_number(conn, worker_id):
    """
    Returns which practice the worker needs (as 0 ... N)

    :param conn: The HappyBase connection object.
    :param worker_id: The Worker ID (from MTurk) as a string.
    :return: An integer corresponding to the practice the worker needs (starting from the top 'row')
    """
    table = conn.table(WORKER_TABLE)
    rowData = table.row(worker_id)
    return int(rowData.get('status:num_practices_attempted_this_week', '0'))


def worker_is_banned(conn, worker_id):
    """
    Determines whether or not the worker is banned.

    :param conn: The HappyBase connection object.
    :param worker_id: The Worker ID (from MTurk), as a string.
    :return: True if the worker is banned, False otherwise.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(worker_id, columns=['status:is_banned'])
    return data.get('status:is_banned', FALSE) == TRUE


def get_worker_ban_time_reason(conn, worker_id):
    """
    Returns the length of remaining time this worker is banned along with the reason as a tuple.

    :param conn: The HappyBase connection object.
    :param worker_id: The Worker ID (from MTurk), as a string.
    :return: The time until the ban expires and the reason for the ban.
    """
    if not worker_is_banned(conn, worker_id):
        return None, None
    table = conn.table(WORKER_TABLE)
    data = table.row(worker_id, columns=['status:ban_length', 'status:ban_reason'], include_timestamp=True)
    ban_time, timestamp = data.get('status:ban_length', (DEFAULT_BAN_LENGTH, 0))
    ban_reason, _ = data.get('status:ban_reason', (DEFAULT_BAN_REASON, 0))
    return _get_timedelta_string(int(ban_time * 1000), timestamp), ban_reason


def worker_attempted_this_week(conn, worker_id):
    """
    Returns the number of tasks this worker has attempted this week.

    :param conn: The HappyBase connection object.
    :param worker_id: The worker ID, as a string.
    :return: Integer, the number of tasks the worker has attempted this week.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(worker_id, columns=['stats:num_attempted_this_week'])
    return int(data.get('stats:num_attempted_this_week', '0'))


def worker_attempted_too_much(conn, worker_id):
    """
    Returns True if the worker has attempted too many tasks.

    :param conn: The HappyBase connection object.
    :param worker_id: The worker ID, as a string.
    :return: True if the worker has attempted too many tasks, otherwise False.
    """
    return worker_attempted_this_week(conn, worker_id) > MAX_ATTEMPTS_PER_WEEK


def worker_weekly_rejected(conn, worker_id):
    """
    Returns the rejection-to-acceptance ratio for this worker for this week.

    :param conn: The HappyBase connection object.
    :param worker_id: The worker ID, as a string.
    :return: Float, the number of tasks rejected divided by the number of tasks accepted.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(worker_id, columns=['stats:num_rejected_this_week'])
    return int(data.get('stats:num_rejected_this_week', '0'))


def worker_weekly_reject_accept_ratio(conn, worker_id):
    """
    Returns the rejection-to-acceptance ratio for this worker for this week.

    :param conn: The HappyBase connection object.
    :param worker_id: The worker ID, as a string.
    :return: Float, the number of tasks rejected divided by the number of tasks accepted.
    """
    table = conn.table(WORKER_TABLE)
    data = table.row(worker_id, columns=['stats:num_accepted_this_week', 'stats:num_rejected_this_week'])
    num_acc = float(data.get('stats:num_accepted_this_week', '0'))
    num_rej = float(data.get('stats:num_rejected_this_week', '0'))
    return num_rej / num_acc


# TASK


def get_task_status(conn, task_id):
    """
    Fetches the status code given a task ID.

    :param conn: The HappyBase connection object.
    :param task_id: The task ID, which is the row key.
    :return: A status code, as defined in conf.
    """
    table = conn.table(TASK_TABLE)
    if not table_has_row(table, task_id):
        return DOES_NOT_EXIST
    task = table.row(task_id)
    if task.get('metadata:is_practice', FALSE) == TRUE:
        return IS_PRACTICE
    if task.get('status:awaiting_serve', FALSE) == TRUE:
        if task.get('status:awaiting_hit_group', FALSE) == TRUE:
            return AWAITING_HIT
        return AWAITING_SERVE
    if task.get('status:pending_completion', FALSE) == TRUE:
        return COMPLETION_PENDING
    if task.get('status:pending_evaluation', FALSE) == TRUE:
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
        scanner = table.scan(columns=['metadata:is_practice'],
                             filter=IS_PRACTICE_FILTER)
        cnt = 0
        for task_id, data in scanner:
            if cnt == practice_n:
                # ideally, it'd check if there is block information for this task, but that requires loading the entire
                # object into memory (unless there's some kind of filter for it)
                return task_id
            cnt += 1
        return None
    else:
        try:
            task_id, data = table.scan(columns=['status:awaiting_serve'],
                                      filter=AWAITING_SERVE_FILTER).next()
        except StopIteration:
            # TODO: regenerate more tasks in this case.
            _log.warning('No tasks available.')
        return task_id


def get_n_awaiting_hit(conn):
    """
    Returns the number of tasks that are awaiting a HIT assignment.

    :param conn: The HappyBase connection object.
    :return: None
    """
    row_filter = general_filter([('status', 'awaiting_serve')],
                                [TRUE], key_only=True)
    scanner = conn.table(TASK_TABLE).scan(filter=row_filter)
    awaiting_hit_cnt = 0
    for task_data in scanner:
        awaiting_hit_cnt += 1
    return awaiting_hit_cnt


def get_n_with_hit_awaiting_serve(conn):
    """
    Returns the number of true tasks that are awaiting serving.

    :param conn: The HappyBase connection object.
    :return: None
    """
    row_filter = general_filter([('status', 'awaiting_hit_type'), ('status', 'awaiting_serve')],
                                [FALSE, TRUE], key_only=True)
    scanner = conn.table(TASK_TABLE).scan(filter=row_filter)
    awaiting_serve_cnt = 0
    for task_data in scanner:
        awaiting_serve_cnt += 1
    return awaiting_serve_cnt


def get_task_blocks(conn, task_id):
    """
    Returns the task blocks, as a list of dictionaries, appropriate for make_html.

    :param conn: The HappyBase connection object.
    :param task_id: The task ID, as a string.
    :return: List of blocks represented as dictionaries, if there is a problem returns None.
    """
    table = conn.table(TASK_TABLE)
    pickled_blocks = table.row(task_id, columns=['blocks:c1']).get('blocks:c1', None)
    if pickled_blocks is None:
        return None
    return loads(pickled_blocks)


def task_is_practice(conn, task_id):
    """
    Indicates whether or not the task in question is a practice.

    :param conn: The HappyBase connection object.
    :param task_id: The task ID, as a string.
    :return: Boolean. Returns True if the task specified by the task ID is a practice, otherwise false.
    """
    table = conn.table(TASK_TABLE)
    return table.get(task_id).get('metadata:is_practice', None)


# HIT TYPES


def get_hit_type_info(conn, hit_type_id):
    """
    Returns the information for a hit_type_id.

    :param conn: The HappyBase connection object.
    :param hit_type_id: The HIT type ID, as provided by mturk.
    :return: The HIT Type information, as a dictionary. If this hit_type_id does not exist, returns an empty dictionary.
    """
    table = conn.table(HIT_TYPE_TABLE)
    return table.get(hit_type_id)


def hit_type_matches(conn, hit_type_id, task_attribute, image_attributes):
    """
    Indicates whether or not the hit is an appropriate match for.

    NOTE:
        This is a bit wonky, as the image_attributes for task types (which

    :param conn: The HappyBase connection object.
    :param hit_type_id: The HIT type ID, as provided by mturk (see webserver.mturk.register_hit_type_mturk).
    :param task_attribute: The task attribute for tasks that are HITs assigned to this HIT type.
    :param image_attributes: The image attributes for tasks that are HITs assigned to this HIT type.
    :return: True if hit_type_id corresponds to a HIT type that has the specified task attribute and the specified
             image attributes.
    """
    type_info = get_hit_type_info(conn, hit_type_id)
    if type_info == {}:
        _log.warning('No such hit type')
        return False
    if task_attribute != type_info.get('status:task_attribute', None):
        return False
    try:
        db_hit_type_image_attributes = loads(type_info.get('metadata:image_attributes', dumps(IMAGE_ATTRIBUTES)))
    except:
        db_hit_type_image_attributes = set()
    if set(image_attributes) != db_hit_type_image_attributes:
        return False
    return True


def get_active_hit_types(conn):
    """
    Obtains active hit types which correspond to non-practice tasks.

    :param conn: The HappyBase connection object.
    :return: An iterator over active hit types.
    """
    row_filter = general_filter([('status', 'active'), ('metadata', 'is_practice')],
                                values=[TRUE, FALSE], key_only=True)
    return conn.table(HIT_TYPE_TABLE).scan(filter=row_filter)


def get_active_practice_hit_types(conn):
    """
    Obtains active hit types that correspond to practice tasks.

    :param conn: The HappyBase connection object.
    :return: An iterator over active practice hit types.
    """
    row_filter = general_filter([('status', 'active'), ('metadata', 'is_practice')],
                                values=[TRUE, TRUE], key_only=True)
    return conn.table(HIT_TYPE_TABLE).scan(filter=row_filter)


# GENERAL QUERIES


def table_exists(conn, table_name):
    """
    Checks if a table exists.

    :param conn: The HappyBase connection object.
    :param table_name: The name of the table to check for existence.
    :return: True if table exists, false otherwise.
    """
    return table_name in conn.tables()


def table_has_row(table, row_key):
    """
    Determines if a table has a defined row key or not.

    :param table: A HappyBase table object.
    :param row_key: The desired row key, as a string.
    :return: True if key exists, false otherwise.
    """
    scan = table.scan(row_start=row_key, filter='KeyOnlyFilter() AND FirstKeyOnlyFilter()', limit=1)
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
    for item in x:
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


def get_n_active_images(conn, image_attributes=IMAGE_ATTRIBUTES):
    """
    Gets a count of active images.

    :param conn: The HappyBase connection object.
    :param image_attributes: The image attributes that the images considered must satisfy.
    :return: An integer, the number of active images.
    """
    table = conn.table(IMAGE_TABLE)
    # note: do NOTE use binary prefix, because 1 does not correspond to the string 1, but to the binary 1.
    scanner = table.scan(columns=['metadata:is_active'],
                        filter=attribute_image_filter(image_attributes, only_active=True))
    active_image_count = 0
    for item in scanner:
        active_image_count += 1
    return active_image_count


def image_is_active(conn, image_id):
    """
    Returns True if an image has been registered into the database and is an active image.

    :param conn: The HappyBase connection object.
    :param image_id: The image ID, which is the row key.
    :return: True if the image is active. False otherwise.
    """
    table = conn.table(IMAGE_TABLE)
    is_active = table.row(image_id, columns=['metadata:is_active']).get('metadata:is_active', None)
    if is_active == TRUE:
        return True
    else:
        return False


def image_get_min_seen(conn, image_attributes=IMAGE_ATTRIBUTES):
    """
    Returns the number of times the least-seen image has been seen. (I.e., the number of tasks it has been featured in.

    NOTES: This only applies to active images.

    :param conn: The HappyBase connection object.
    :param image_attributes: The image attributes that the images considered must satisfy.
    :return: Integer, the min number of times seen.
    """
    obs_min = np.inf
    table = conn.table(IMAGE_TABLE)
    # note that if we provide a column argument, rows without this column are not emitted.
    scanner = table.scan(columns=['stats:num_times_seen'],
                         filter=attribute_image_filter(image_attributes, only_active=True))
    been_seen = 0
    for row_key, row_data in scanner:
        been_seen += 1
        cur_seen = row_data.get('stats:num_times_seen', 0)
        if cur_seen < obs_min:
            obs_min = cur_seen
        if obs_min == 0:
            return 0  # it can't go below 0, so you can cut the scan short
    if not been_seen:
        return 0 # this is an edge case, where none of the images have been seen.
    return obs_min


def get_n_images(conn, n, base_prob=None, image_attributes=IMAGE_ATTRIBUTES):
    """
    Returns n images from the database, sampled according to some probability. These are fit for use in design
    generation.

    NOTES:
        If not given, the base_prob is defined to be n / N, where N is the number of active images.

    :param conn: The HappyBase connection object.
    :param n: Number of images to choose.
    :param base_prob: The base probability of selecting any image.
    :param image_attributes: The image attributes that the images return must satisfy.
    :return: A list of image IDs.
    """
    n_active = get_n_active_images(conn, image_attributes)
    if n > n_active:
        _log.warning('Insufficient number of active images, activating %i more.' % ACTIVATION_CHUNK_SIZE)
        dbset.activate_n_images(conn, ACTIVATION_CHUNK_SIZE, image_attributes)
    min_seen = image_get_min_seen(conn, image_attributes)
    if min_seen > SAMPLES_REQ_PER_IMAGE(n_active):
        _log.warning('Images are sufficiently sampled, activating %i images.' % ACTIVATION_CHUNK_SIZE)
        dbset.activate_n_images(conn, ACTIVATION_CHUNK_SIZE, image_attributes)
    if base_prob is None:
        base_prob = float(n) / n_active
    p = _prob_select(base_prob, min_seen)
    table = conn.table(IMAGE_TABLE)
    images = set()
    while len(images) < n:
        # repeatedly scan the database, selecting images -- don't bother selecting the num_times_seen column, since it
        # wont be defined for images that have yet to be seen.
        scanner = table.scan(filter=attribute_image_filter(image_attributes, only_active=True))
        for row_key, row_data in scanner:
            cur_seen = row_data.get('stats:num_times_seen', 0)
            if np.random.rand() < p(cur_seen):
                images.add(row_key)
    return list(images)


# TASK DESIGN STUFF


def get_design(conn, n, t, j, image_attributes=IMAGE_ATTRIBUTES):
    """
    Returns a task design, as a series of tuples of images. This is based directly on generate/utils/get_design, which
    should be consulted for reference on the creation of Steiner systems.

    This extends get_design by not only checking against co-occurence within the task, but also globally across all
    tasks by invoking _tuple_permitted.

    :param conn: The HappyBase connection object.
    :param n: The number of distinct elements involved in the experiment.
    :param t: The number of elements to present each trial.
    :param j: The number of times each element should appear during the experiment.
    :param image_attributes: The attributes that images must have to be into the study. Images must have any of
                             these attributes.
    :return: A list of tuples representing each subset. Elements may be randomized within trial and subset order may
    be randomized without consequence.
    """
    occ = np.zeros(n)  # an array which stores the number of times an image has been used.
    design = []
    images = get_n_images(conn, n, 0.05, image_attributes=image_attributes)
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
                obs.add(pair_to_tuple(x1, x2))
            for i in c:
                occ[i] += 1
            design.append(cur_tuple)
    if not np.min(occ) >= j:
        return None
    return design


def get_task(conn, n, t, j, n_keep_blocks=None, n_reject_blocks=None, prompt=None, practice=False,
             attribute=ATTRIBUTE, random_segment_order=RANDOMIZE_SEGMENT_ORDER, image_attributes=IMAGE_ATTRIBUTES,
             hit_type_id=None):
    """
    Creates a new task, by calling get_design and then arranging those tuples into keep and reject blocks, and then
    registering it in the database. Additionally, you may specify which hit_type_id this task should be for. If this
    is the case, then it overwrites:
        task_attribute
        image_attributes
        practice

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
    :param practice: Boolean, whether or not this task is a practice.
    :param attribute: The task attribute.
    :param random_segment_order: Whether or not to randomize block ordering.
    :param image_attributes: The set of attributes that the images from this task have.
    :param hit_type_id: The HIT type ID, as provided by MTurk and as findable in the database.
    :return: None.
    """
    if practice:
        task_id = practice_id_gen()
    else:
        task_id = task_id_gen()
    if hit_type_id:
        hit_type_info = get_hit_type_info(conn, hit_type_id)
        practice = hit_type_info.get('metadata:is_practice', FALSE) == TRUE
        attribute = hit_type_info.get('metadata:attribute', ATTRIBUTE)
        image_attributes = list(loads(hit_type_info.get('metadata:image_attributes', dumps(IMAGE_ATTRIBUTES))))
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
    image_tuples = get_design(conn, n, t, j, image_attributes=image_attributes)
    # arrange them into blocks
    keep_tuples = [image_tuples[i:i+n] for i in xrange(0, len(image_tuples), n_keep_blocks)]
    reject_tuples = [image_tuples[i:i+n] for i in xrange(0, len(image_tuples), n_reject_blocks)]
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
    exp_seq = [[x['type'], [tuple(y) for y in x['images']]] for x in blocks]
    dbset.register_task(conn, task_id, exp_seq, attribute, blocks=blocks, is_practice=practice, check_ims=True,
                        image_attributes=image_attributes)


def get_active_hit_type_id_for_task(conn, task_id):
    """
    Returns the ID for an appropriate HIT type given the task. This is potentially expensive, but will be done
    offline.

    :param conn: The HappyBase connection object.
    :param task_id: The task ID, as a string.
    :return: An appropriate HIT type ID for this task, otherwise None. Returns the first one it finds.
    """
    task_info = conn.table(TASK_TABLE).row(task_id)
    cur_task_is_practice = task_info.get('metadata:is_practice', FALSE) == TRUE
    task_attribute = task_info.get('metadata:attribute', ATTRIBUTE)
    image_attributes = loads(task_info.get('metadata:image_attributes', dumps(IMAGE_ATTRIBUTES)))
    if cur_task_is_practice:
        scanner = get_active_practice_hit_types(conn)
    else:
        scanner = get_active_hit_types(conn)
    for hit_type_id, _ in scanner:
        if hit_type_matches(conn, hit_type_id, task_attribute, image_attributes):
            return hit_type_id
    return None
