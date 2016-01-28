"""
We're making a major move, towards defining the methods in Get, Set, and MTurk as Classes, rather than individual
functions.
"""

from conf import *
from dill import dumps
from dill import loads
from itertools import combinations as comb
from datetime import datetime
import time
import warnings

"""
LOGGING
"""

_log = logger.setup_logger(__name__)

"""
PRIVATE METHODS
"""


def _get_pair_key(image1, image2):
    """
    Returns a row key for a given pair. Row keys for pairs are the image IDs, separated by a comma, and sorted
    alphabetically. The inputs need not be sorted alphabetically.

    :param image1: Image 1 ID.
    :param image2: Image 2 ID.
    :return: The pair row key, as a string.
    """
    return ','.join(pair_to_tuple(image1, image2))


def _get_preexisting_pairs_slow(conn, images):
    """
    Returns all pairs that have already occurred among a list of images.

    NOTE:
        There is an open question about to the approach here, with respect to which one is more efficient:
            (1) Iterate over each pair in images, and see if they exist.
            (2) Select all pairs for each image with a prefix and look for pairs where the other image is also in
            images.
        For now, we will use (1), for its conceptual simplicity.

        This is SLOW. Can we speed it up?

    :param conn: The HappyBase connection object.
    :param images: A iterable of image IDs
    :return: A set of tuples of forbidden image ID pairs.
    """
    tot_to_scan = 0.5 * len(images) * (len(images) * 1)
    to_scan_brks = tot_to_scan / 10
    _log.debug('Scanning over %i possible pairs' % tot_to_scan)
    found_pairs = set()
    for n, (im1, im2) in enumerate(comb(images, 2)):
        if not n % to_scan_brks and n > 0:
            _log.debug('%.0f%% done.' % (100. * n / tot_to_scan))
        pair_id = _get_pair_key(im1, im2)
        if _pair_exists(conn, pair_id):
            found_pairs.add(pair_to_tuple(im1, im2))
    return found_pairs


def _get_preexisting_pairs(conn, images):
    """
    Returns all pairs that have already occurred among a list of images. The original implementation is potentially
    more robust (_get_preexisting_pairs_slow), but has polynomial complexity, which is obviously undesirable. The
    conceptual similarity (as mentioned in the original implementation) is not worth the added time complexity,
    especially when we may be generating thousands of tasks.

    :param conn: The HappyBase connection object.
    :param images: A iterable of image IDs
    :return: A set of tuples of forbidden image ID pairs.
    """
    found_pairs = set()
    table = conn.table(IMAGE_TABLE)
    im_set = set(images)
    for im1 in images:
        scanner = table.scan(row_prefix=im1, columns=['metadata:im_id1', 'metadata:im_id2'])
        for row_key, row_data in scanner:
            # don't use get with this, we want it to fail hard if it does.
            c_im1 = row_data['metadata:im_id1']
            c_im2 = row_data['metadata:im_id2']
            if c_im1 in im_set and c_im2 in im_set:
                found_pairs.add(pair_to_tuple(c_im1, c_im2))
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
    return Get.table_has_row(table, pair_key)


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
    expire_time = ban_issued + ban_length
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


def _create_table(conn, table, families, clobber):
    """
    General create table function.

    :param conn: The HappyBase connection object.
    :param table: The table name.
    :param families: The table families, see conf.py
    :param clobber: Boolean, if true will erase old workers table if it exists. [def: False]
    :return: True if table was created. False otherwise.
    """
    if table in conn.tables():
        if not clobber:
            # it exists and you can't do anything about it.
            return False
        # delete the table
        conn.delete_table(table, disable=True)
    conn.create_table(table, families)
    return table in conn.tables()


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
    return {prefix + ':%i' % n: v for n, v in enumerate(data)}


def _get_image_dict(image_url):
    """
    Returns a dictionary for image data, appropriate for inputting into the image table.

    :param image_url: The URL of the image, as a string.
    :return: If the image can be found and opened, a dictionary. Otherwise None.
    """
    width, height = get_im_dims(image_url)
    if width is None:
        return None
    aspect_ratio = '%.3f'%(float(width) / height)
    im_dict = {'metadata:width': width, 'metadata:height': height, 'metadata:aspect_ratio': aspect_ratio,
               'metadata:url': image_url, 'metadata:is_active': FALSE}
    return _conv_dict_vals(im_dict)


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
    pair_dict = {'metadata:im_id1': im1, 'metadata:im_id2': im2, 'metadata:task_id': task_id,
                 'metadata:attribute': attribute}
    return _conv_dict_vals(pair_dict)


"""
Main Classes - GET
"""


class Get(object):
    """
    Handles general query events for the task. These are more abstract than the 'set' functions, as there is a larger
    variety of possibilities here.
    """
    def __init__(self, conn):
        """
        :param conn: The HappyBase / HBase connection object.
        :return: A Get instance.
        """
        self.conn = conn
    
    def worker_exists(self, worker_id):
        """
        Indicates whether we have a record of this worker in the database.

        :param worker_id: The Worker ID (from MTurk), as a string.
        :return: True if the there are records of this worker, otherwise false.
        """
        table = self.conn.table(WORKER_TABLE)
        return self.table_has_row(table, worker_id)

    def worker_need_demographics(self, worker_id):
        """
        Indicates whether or not the worker needs demographic information.

        :param worker_id: The Worker ID (from MTurk), as a string.
        :return: True if we need demographic information from the worker. False otherwise.
        """
        table = self.conn.table(WORKER_TABLE)
        row_data = table.row(worker_id)
        if len(row_data.get('demographics:age', '')):
            return True
        else:
            return False

    def worker_need_practice(self, worker_id):
        """
        Indicates whether the worker should be served a practice or a real task.

        :param worker_id: The Worker ID (from MTurk), as a string.
        :return: True if the worker has not passed a practice yet and must be served one. False otherwise.
        """
        table = self.conn.table(WORKER_TABLE)
        row_data = table.row(worker_id)
        return row_data.get('status:passed_practice', FALSE) != TRUE

    def current_worker_practices_number(self, worker_id):
        """
        Returns which practice the worker needs (as 0 ... N)

        :param worker_id: The Worker ID (from MTurk) as a string.
        :return: An integer corresponding to the practice the worker needs (starting from the top 'row')
        """
        table = self.conn.table(WORKER_TABLE)
        row_data = table.row(worker_id)
        return int(row_data.get('status:num_practices_attempted_this_week', '0'))

    def worker_is_banned(self, worker_id):
        """
        Determines whether or not the worker is banned.

        :param worker_id: The Worker ID (from MTurk), as a string.
        :return: True if the worker is banned, False otherwise.
        """
        table = self.conn.table(WORKER_TABLE)
        data = table.row(worker_id, columns=['status:is_banned'])
        return data.get('status:is_banned', FALSE) == TRUE

    def get_worker_ban_time_reason(self, worker_id):
        """
        Returns the length of remaining time this worker is banned along with the reason as a tuple.

        :param worker_id: The Worker ID (from MTurk), as a string.
        :return: The time until the ban expires and the reason for the ban.
        """
        if not self.worker_is_banned(worker_id):
            return None, None
        table = self.conn.table(WORKER_TABLE)
        data = table.row(worker_id, columns=['status:ban_length', 'status:ban_reason'], include_timestamp=True)
        ban_time, timestamp = data.get('status:ban_length', (DEFAULT_BAN_LENGTH, 0))
        ban_reason, _ = data.get('status:ban_reason', (DEFAULT_BAN_REASON, 0))
        return _get_timedelta_string(int(ban_time * 1000), timestamp), ban_reason

    def worker_attempted_this_week(self, worker_id):
        """
        Returns the number of tasks this worker has attempted this week.

        :param worker_id: The worker ID, as a string.
        :return: Integer, the number of tasks the worker has attempted this week.
        """
        table = self.conn.table(WORKER_TABLE)
        data = table.row(worker_id, columns=['stats:num_attempted_this_week'])
        return int(data.get('stats:num_attempted_this_week', '0'))

    def worker_attempted_too_much(self, worker_id):
        """
        Returns True if the worker has attempted too many tasks.

        :param worker_id: The worker ID, as a string.
        :return: True if the worker has attempted too many tasks, otherwise False.
        """
        _log.warning('DEPRECATED: This functionality is now performed implicitly by the MTurk task structure')
        return self.worker_attempted_this_week(worker_id) > (7 * MAX_SUBMITS_PER_DAY)

    def worker_weekly_rejected(self, worker_id):
        """
        Returns the rejection-to-acceptance ratio for this worker for this week.

        :param worker_id: The worker ID, as a string.
        :return: Float, the number of tasks rejected divided by the number of tasks accepted.
        """
        table = self.conn.table(WORKER_TABLE)
        return table.counter_get(worker_id, 'stats:num_rejected_this_week')

    def worker_weekly_reject_accept_ratio(self, worker_id):
        """
        Returns the rejection-to-acceptance ratio for this worker for this week.

        :param worker_id: The worker ID, as a string.
        :return: Float, the number of tasks rejected divided by the number of tasks accepted.
        """
        table = self.conn.table(WORKER_TABLE)
        num_acc = float(table.counter_get(worker_id, 'stats:num_accepted_this_week'))
        num_rej = float(table.counter_get(worker_id, 'stats:num_rejected_this_week'))
        return num_rej / num_acc

    # TASK
    
    def get_task_status(self, task_id):
        """
        Fetches the status code given a task ID.

        :param task_id: The task ID, which is the row key.
        :return: A status code, as defined in conf.
        """
        table = self.conn.table(TASK_TABLE)
        if not self.table_has_row(table, task_id):
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

    def get_available_task(self, practice=False, practice_n=0):
        """
        Returns an available task.

        :param practice: Whether or not to fetch a practice task. [optional]
        :param practice_n: Which practice to serve, starting from 0. [optional]
        :return: The task ID for an available task. If there is no task available, it returns None.
        """
        table = self.conn.table(TASK_TABLE)
        if practice:
            scanner = table.scan(columns=['metadata:is_practice'],
                                 filter=IS_PRACTICE_FILTER)
            cnt = 0
            for task_id, data in scanner:
                if cnt == practice_n:
                    # ideally, it'd check if there is block information for this task, but that requires loading the
                    # entire object into memory (unless there's some kind of filter for it)
                    return task_id
                cnt += 1
            return None
        else:
            try:
                task_id, data = table.scan(columns=['status:awaiting_serve'],
                                           filter=AWAITING_SERVE_FILTER).next()
                return task_id
            except StopIteration:
                # TODO: regenerate more tasks in this case.
                _log.warning('No tasks available.')
                return None

    def get_n_awaiting_hit(self):
        """
        Returns the number of tasks that are awaiting a HIT assignment.

        :return: None
        """
        row_filter = general_filter([('status', 'awaiting_serve')],
                                    [TRUE], key_only=True)
        scanner = self.conn.table(TASK_TABLE).scan(filter=row_filter)
        awaiting_hit_cnt = 0
        for _ in scanner:
            awaiting_hit_cnt += 1
        return awaiting_hit_cnt

    def get_n_with_hit_awaiting_serve(self):
        """
        Returns the number of true tasks that are awaiting serving.

        :return: None
        """
        row_filter = general_filter([('status', 'awaiting_hit_type'), ('status', 'awaiting_serve')],
                                    [FALSE, TRUE], key_only=True)
        scanner = self.conn.table(TASK_TABLE).scan(filter=row_filter)
        awaiting_serve_cnt = 0
        for _ in scanner:
            awaiting_serve_cnt += 1
        return awaiting_serve_cnt

    def get_task_blocks(self, task_id):
        """
        Returns the task blocks, as a list of dictionaries, appropriate for make_html.

        :param task_id: The task ID, as a string.
        :return: List of blocks represented as dictionaries, if there is a problem returns None.
        """
        table = self.conn.table(TASK_TABLE)
        pickled_blocks = table.row(task_id, columns=['blocks:c1']).get('blocks:c1', None)
        if pickled_blocks is None:
            return None
        return loads(pickled_blocks)

    def task_is_practice(self, task_id):
        """
        Indicates whether or not the task in question is a practice.

        :param task_id: The task ID, as a string.
        :return: Boolean. Returns True if the task specified by the task ID is a practice, otherwise false.
        """
        table = self.conn.table(TASK_TABLE)
        return table.row(task_id).get('metadata:is_practice', None)

    def task_is_acceptable(self, task_id):
        """
        Indicates whether or not a task is acceptable.

        :param task_id: The task ID, as a string.
        :return: True if the task is acceptable, False otherwise.
        """
        # TODO: Implement this.
        raise NotImplementedError()

    # HIT TYPES
    
    def get_hit_type_info(self, hit_type_id):
        """
        Returns the information for a hit_type_id.

        :param hit_type_id: The HIT type ID, as provided by mturk.
        :return: The HIT Type information, as a dictionary. If this hit_type_id does not exist, returns an empty dictionary.
        """
        table = self.conn.table(HIT_TYPE_TABLE)
        return table.row(hit_type_id)

    def hit_type_matches(self, hit_type_id, task_attribute, image_attributes):
        """
        Indicates whether or not the hit is an appropriate match for.
    
        NOTE:
            This is a bit wonky, as the image_attributes for task types (which

        :param hit_type_id: The HIT type ID, as provided by mturk (see webserver.mturk.register_hit_type_mturk).
        :param task_attribute: The task attribute for tasks that are HITs assigned to this HIT type.
        :param image_attributes: The image attributes for tasks that are HITs assigned to this HIT type.
        :return: True if hit_type_id corresponds to a HIT type that has the specified task attribute and the specified
                 image attributes.
        """
        type_info = self.get_hit_type_info(hit_type_id)
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

    def get_active_hit_types(self):
        """
        Obtains active hit types which correspond to non-practice tasks.

        :return: An iterator over active hit types.
        """
        row_filter = general_filter([('status', 'active'), ('metadata', 'is_practice')],
                                    values=[TRUE, FALSE], key_only=True)
        return self.conn.table(HIT_TYPE_TABLE).scan(filter=row_filter)

    def get_active_practice_hit_types(self):
        """
        Obtains active hit types that correspond to practice tasks.

        :return: An iterator over active practice hit types.
        """
        row_filter = general_filter([('status', 'active'), ('metadata', 'is_practice')],
                                    values=[TRUE, TRUE], key_only=True)
        return self.conn.table(HIT_TYPE_TABLE).scan(filter=row_filter)

    # GENERAL QUERIES

    def table_exists(self, table_name):
        """
        Checks if a table exists.

        :param table_name: The name of the table to check for existence.
        :return: True if table exists, false otherwise.
        """
        return table_name in self.conn.tables()

    @staticmethod
    def table_has_row(table, row_key):
        """
        Determines if a table has a defined row key or not.
    
        :param table: A HappyBase table object.
        :param row_key: The desired row key, as a string.
        :return: True if key exists, false otherwise.
        """
        scan = table.scan(row_start=row_key, filter='KeyOnlyFilter() AND FirstKeyOnlyFilter()', limit=1)
        return next(scan, None) is not None

    @staticmethod
    def get_num_items(table):
        """
        Counts the number of rows in a table.
    
        NOTES: This is likely to be pretty inefficient.
    
        :param table: A HappyBase table object.
        :return: An integer, the number of rows in the object.
        """
        x = table.scan(filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
        tot_ims = 0
        for _ in x:
            tot_ims += 1
        return tot_ims

    @staticmethod
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

    def get_n_active_images_count(self, image_attributes=IMAGE_ATTRIBUTES):
        """
        Gets a count of active images.

        :param image_attributes: The image attributes that the images considered must satisfy.
        :return: An integer, the number of active images.
        """
        table = self.conn.table(IMAGE_TABLE)
        # note: do NOTE use binary prefix, because 1 does not correspond to the string 1, but to the binary 1.
        scanner = table.scan(columns=['metadata:is_active'],
                             filter=attribute_image_filter(image_attributes, only_active=True))
        active_image_count = 0
        for _ in scanner:
            active_image_count += 1
        return active_image_count

    def image_is_active(self, image_id):
        """
        Returns True if an image has been registered into the database and is an active image.

        :param image_id: The image ID, which is the row key.
        :return: True if the image is active. False otherwise.
        """
        table = self.conn.table(IMAGE_TABLE)
        is_active = table.row(image_id, columns=['metadata:is_active']).get('metadata:is_active', None)
        if is_active == TRUE:
            return True
        else:
            return False

    def image_get_min_seen(self, image_attributes=IMAGE_ATTRIBUTES):
        """
        Returns the number of times the least-seen image has been seen. (I.e., the number of tasks it has been featured in.
    
        NOTES: This only applies to active images.

        :param image_attributes: The image attributes that the images considered must satisfy.
        :return: Integer, the min number of times seen.
        """
        obs_min = np.inf
        table = self.conn.table(IMAGE_TABLE)
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
            return 0  # this is an edge case, where none of the images have been seen.
        return obs_min
    
    def get_n_images(self, n, base_prob=None, image_attributes=IMAGE_ATTRIBUTES):
        """
        Returns n images from the database, sampled according to some probability. These are fit for use in design
        generation.
    
        NOTES:
            If not given, the base_prob is defined to be n / N, where N is the number of active images.

        :param n: Number of images to choose.
        :param base_prob: The base probability of selecting any image.
        :param image_attributes: The image attributes that the images return must satisfy.
        :return: A list of image IDs, unless it cannot get enough images -- then returns None.
        """
        n_active = self.get_n_active_images_count(image_attributes)
        if n > n_active:
            _log.warning('Insufficient number of active images, activating %i more.' % ACTIVATION_CHUNK_SIZE)
            # TODO: Add a statemon here?
            return None
        min_seen = self.image_get_min_seen(image_attributes)
        if min_seen > SAMPLES_REQ_PER_IMAGE(n_active):
            _log.warning('Images are sufficiently sampled, activating %i images.' % ACTIVATION_CHUNK_SIZE)
            # TODO: Add a statemon here?
            return None
        if base_prob is None:
            base_prob = float(n) / n_active
        p = _prob_select(base_prob, min_seen)
        table = self.conn.table(IMAGE_TABLE)
        images = set()
        while len(images) < n:
            # repeatedly scan the database, selecting images -- don't bother selecting the num_times_seen column, since
            #  it wont be defined for images that have yet to be seen.
            scanner = table.scan(filter=attribute_image_filter(image_attributes, only_active=True))
            for row_key, row_data in scanner:
                cur_seen = row_data.get('stats:num_times_seen', 0)
                if np.random.rand() < p(cur_seen):
                    images.add(row_key)
        return list(images)

    # TASK DESIGN STUFF
    
    def gen_design(self, n, t, j, image_attributes=IMAGE_ATTRIBUTES):
        """
        Returns a task design, as a series of tuples of images. This is based directly on generate/utils/get_design,
        which should be consulted for reference on the creation of Steiner systems.
    
        This extends get_design by not only checking against co-occurrence within the task, but also globally across all
        tasks by invoking _tuple_permitted.

        :param n: The number of distinct elements involved in the experiment.
        :param t: The number of elements to present each trial.
        :param j: The number of times each element should appear during the experiment.
        :param image_attributes: The attributes that images must have to be into the study. Images must have any of
                                 these attributes.
        :return: A list of tuples representing each subset. Elements may be randomized within trial and subset order may
                 be randomized without consequence. If there is not enough images to generate, returns None.
        """
        occ = np.zeros(n)  # an array which stores the number of times an image has been used.
        design = []
        images = self.get_n_images(n, 0.05, image_attributes=image_attributes)
        if images is None:
            _log.error('Unable to fetch images to generate design!')
            return None
        np.random.shuffle(images)  # shuffle the images (remember its in-place! >.<)
        obs = _get_preexisting_pairs(self.conn, images)  # the set of observed tuples
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

    def gen_task(self, n, t, j, n_keep_blocks=None, n_reject_blocks=None, prompt=None, practice=False,
                 attribute=ATTRIBUTE, random_segment_order=RANDOMIZE_SEGMENT_ORDER, image_attributes=IMAGE_ATTRIBUTES,
                 hit_type_id=None):
        """
        Creates a new task, by calling gen_design and then arranging those tuples into keep and reject blocks.
        Additionally, you may specify which hit_type_id this task should be for. If this
        is the case, it overwrites:
            task_attribute
            image_attributes
            practice
        In accordance with the design philosophy of segregating function, gen_task does not attempt to modify the
        database. Instead, it returns elements that befit a call to Set's register_task.
    
        NOTES:
            Keep blocks always come first, after which they alternate between Keep / Reject. If the
            RANDOMIZE_SEGMENT_ORDER option is true, then the segments order will be randomized.
    
            The randomization has to be imposed here, along with all other order decisions, because the database assumes
            that data from mechanical turk (i.e., as determined by the task HTML) are in the same order as the data in
            the database.

            This does NOT register the task. It returns a dictionary that befits dbset, but does not do it itself.

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
        :return: task_id, exp_seq, attribute, register_task_kwargs. On failure, returns None.
        """
        if practice:
            task_id = practice_id_gen()
        else:
            task_id = task_id_gen()
        if hit_type_id:
            hit_type_info = self.get_hit_type_info(hit_type_id)
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
        image_tuples = self.gen_design(n, t, j, image_attributes=image_attributes)
        if image_tuples is None:
            return None, None, None, None
        # arrange them into blocks
        keep_tuples = [x for x in chunks(image_tuples, n_keep_blocks)]
        reject_tuples = [x for x in chunks(image_tuples, n_reject_blocks)]
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
        while len(keep_blocks) or len(reject_blocks):
            if len(keep_blocks):
                blocks.append(keep_blocks.pop(0))
            if len(reject_blocks):
                blocks.append(reject_blocks.pop(0))
        if random_segment_order:
            np.random.shuffle(blocks)
        # define expSeq
        # annoying expSeq expects image tuples...
        exp_seq = [[x['type'], [tuple(y) for y in x['images']]] for x in blocks]
        register_task_kwargs = {'blocks': blocks, 'is_practice': practice, 'check_ims': True,
                                'image_attributes': image_attributes}
        return task_id, exp_seq, attribute, register_task_kwargs

    def get_active_hit_type_id_for_task(self, task_id):
        """
        Returns the ID for an appropriate HIT type given the task. This is potentially expensive, but will be done
        offline.

        :param task_id: The task ID, as a string.
        :return: An appropriate HIT type ID for this task, otherwise None. Returns the first one it finds.
        """
        task_info = self.conn.table(TASK_TABLE).row(task_id)
        cur_task_is_practice = task_info.get('metadata:is_practice', FALSE) == TRUE
        task_attribute = task_info.get('metadata:attribute', ATTRIBUTE)
        image_attributes = loads(task_info.get('metadata:image_attributes', dumps(IMAGE_ATTRIBUTES)))
        if cur_task_is_practice:
            scanner = self.get_active_practice_hit_types()
        else:
            scanner = self.get_active_hit_types()
        for hit_type_id, _ in scanner:
            if self.hit_type_matches(hit_type_id, task_attribute, image_attributes):
                return hit_type_id
        return None

    def worker_autoban_check(self, worker_id, duration=None):
        """
        Checks that the worker should be autobanned.

        :param conn: The HappyBase connection object.
        :param worker_id: The worker ID, as a string.
        :return: True if the worker should be autobanned, False otherwise.
        """
        if self.worker_weekly_rejected(worker_id) > MIN_REJECT_AUTOBAN_ELIGIBLE:
            if self.worker_weekly_reject_accept_ratio(self, worker_id) > AUTOBAN_REJECT_ACCEPT_RATIO:
                return True
        return False


"""
Main Classes - SET
"""


class Set(object):
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
    def __init__(self, conn):
        """
        :param conn: The HappyBase / HBase connection object.
        :return: A Get instance.
        """
        self.conn = conn

    def _image_is_active(self, image_id):
        """
        Returns True if an image has been registered into the database and is an active image.

        NOTES:
            Private version of Get method, for use internally for methods of Set.

        :param image_id: The image ID, which is the row key.
        :return: True if the image is active. False otherwise.
        """
        table = self.conn.table(IMAGE_TABLE)
        is_active = table.row(image_id, columns=['metadata:is_active']).get('metadata:is_active', None)
        if is_active == TRUE:
            return True
        else:
            return False

    @staticmethod
    def _table_has_row(table, row_key):
        """
        Determines if a table has a defined row key or not.

        NOTES:
            Private version of Get method, for use internally for methods of Set.

        :param table: A HappyBase table object.
        :param row_key: The desired row key, as a string.
        :return: True if key exists, false otherwise.
        """
        scan = table.scan(row_start=row_key, filter='KeyOnlyFilter() AND FirstKeyOnlyFilter()', limit=1)
        return next(scan, None) is not None

    def _get_task_status(self, task_id):
        """
        Fetches the status code given a task ID.

        NOTES:
            Private version of Get method, for use internally for methods of Set.

        :param task_id: The task ID, which is the row key.
        :return: A status code, as defined in conf.
        """
        table = self.conn.table(TASK_TABLE)
        if not self._table_has_row(table, task_id):
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

    def create_worker_table(self, clobber=False):
        """
        Creates a workers table, with names based on conf.

        :param clobber: Boolean, if true will erase old workers table if it exists. [def: False]
        :return: True if table was created. False otherwise.
        """
        _log.info('Creating worker table.')
        return _create_table(self.conn, WORKER_TABLE, WORKER_FAMILIES, clobber)

    def create_task_table(self, clobber=False):
        """
        Creates a tasks table, with names based on conf.

        :param clobber: Boolean, if true will erase old tasks table if it exists. [def: False]
        :return: True if table was created. False otherwise.
        """
        _log.info('Creating task table.')
        return _create_table(self.conn, TASK_TABLE, TASK_FAMILIES, clobber)

    def create_image_table(self, clobber=False):
        """
        Creates a images table, with names based on conf.

        :param clobber: Boolean, if true will erase old images table if it exists. [def: False]
        :return: True if table was created. False otherwise.
        """
        _log.info('Creating image table.')
        return _create_table(self.conn, IMAGE_TABLE, IMAGE_FAMILIES, clobber)

    def create_pair_table(self, clobber=False):
        """
        Creates a pairs table, with names based on conf.

        :param clobber: Boolean, if true will erase old pairs table if it exists. [def: False]
        :return: True if table was created. False otherwise.
        """
        _log.info('Creating pair table.')
        return _create_table(self.conn, PAIR_TABLE, PAIR_FAMILIES, clobber)

    def create_win_table(self, clobber=False):
        """
        Creates a wins table, with names based on conf.

        :param clobber: Boolean, if true will erase old wins table if it exists. [def: False]
        :return: True if table was created. False otherwise.
        """
        _log.info('Creating win table.')
        return _create_table(self.conn, WIN_TABLE, WIN_FAMILIES, clobber)

    def create_task_type_table(self, clobber=False):
        """
        Creates a HIT type table, that stores information about HIT types.

        :param clobber: Boolean, if true will erase old HIT type table if it exists. [def: False]
        :return: True if table was created. False otherwise.
        """
        _log.info('Creating HIT type table')
        return _create_table(self.conn, HIT_TYPE_TABLE, HIT_TYPE_FAMILIES, clobber)

    def force_regen_tables(self):
        """
        Forcibly rebuilds all tables.

        WARNING: DO NOT USE THIS LIGHTLY!

        :return: True if the tables were all successfully regenerated.
        """
        succ = True
        succ = succ and self.create_worker_table(clobber=True)
        succ = succ and self.create_image_table(clobber=True)
        succ = succ and self.create_pair_table(clobber=True)
        succ = succ and self.create_task_table(clobber=True)
        succ = succ and self.create_win_table(clobber=True)
        succ = succ and self.create_task_type_table(clobber=True)
        return succ

    """
    FUNCTIONS FOR LEGACY DATA
    """

    def register_legacy_task(self, task_id, exp_seq):
        """
        Registers a legacy task.

        :param task_id: A string, the task ID.
        :param exp_seq: A list of lists, in order of presentation, one for each segment. See Notes in register_task()
        :return: None.
        """
        # TODO: implement
        raise NotImplementedError()

    def register_legacy_win(self, winner_id, loser_id, task_id):
        """
        Registers a legacy win.

        :param winner_id: The image_id of the winning image.
        :param loser_id: The image_id of the losing image.
        :param task_id: A string, the task ID.
        :return: None.
        """
        # TODO: implement
        raise NotImplementedError()

    """
    ADDING / CHANGING DATA
    """

    def register_hit_type(self, hit_type_id, task_attribute=ATTRIBUTE, image_attributes=IMAGE_ATTRIBUTES,
                          title=DEFAULT_TASK_NAME, description=DESCRIPTION,
                          reward=DEFAULT_TASK_PAYMENT, assignment_duration=HIT_TYPE_DURATION,
                          keywords=KEYWORDS, auto_approve_delay=AUTO_APPROVE_DELAY,
                          is_practice=FALSE, active=FALSE):
        """
        Registers a HIT type in the database.

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
        hit_type_dict = {'metadata:task_attribute': task_attribute, 'metadata:title': title,
                         'metadata:image_attributes': dumps(set(image_attributes)), 'metadata:description': description,
                         'metadata:reward': reward, 'metadata:assignment_duration': assignment_duration,
                         'metadata:keywords': keywords, 'metadata:auto_approve_delay': auto_approve_delay,
                         'metadata:is_practice': is_practice, 'status:active': active}
        table = self.conn.table(HIT_TYPE_TABLE)
        table.put(hit_type_id, _conv_dict_vals(hit_type_dict))

    def register_task(self, task_id, exp_seq, attribute, blocks=None, is_practice=False, check_ims=False,
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

            Because tasks may expire and need to be reposted as a new HIT or somesuch, do not provide any information about
            the HIT_ID, the Task type ID, etc--in other words, no MTurk-specific information. At this point in the flow
            of information, our knowledge about the task is constrained to be purely local information.

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
        image_list = [] # we also need to store the images as a list in case some need to be incremented more than once
        im_tuples = []
        im_tuple_types = []
        table = self.conn.table(IMAGE_TABLE)
        for seg_type, segment in exp_seq:
            for im_tuple in segment:
                for im in im_tuple:
                    if check_ims:
                        if not self._image_is_active(im):
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
        task_dict['metadata:attributes'] = dumps(set(image_attributes))
        task_dict['status:awaiting_serve'] = TRUE
        task_dict['status:awaiting_hit_type'] = TRUE
        if blocks is None:
            _log.error('No block structure defined for this task - will not be able to load it.')
        else:
            task_dict['blocks:c1'] = dumps(blocks)
        # TODO: Compute forbidden workers?
        # Input the data for the task table
        table = self.conn.table(TASK_TABLE)
        table.put(task_id, _conv_dict_vals(task_dict))
        # Input the data for the pair table.
        if is_practice and not STORE_PRACTICE_PAIRS:
            return
        table = self.conn.table(PAIR_TABLE)
        b = table.batch()
        for pair in pair_list:
            pid = _get_pair_key(pair[0], pair[1])
            b.put(pid, _get_pair_dict(pair[0], pair[1], task_id, attribute))
        b.send()

    def activate_hit_type(self, hit_type_id):
        """
        Activates a HIT type, i.e., indicates that it currently has tasks / HITs. being added to it.

        :param hit_type_id: The HIT type ID, as provided by mturk.
        :return: None
        """
        # TODO: implement
        raise NotImplementedError()

    def deactivate_hit_type(self, hit_type_id):
        """
        Deactivates a HIT type, so that it is no longer accepting new tasks / HITs.

        :param hit_type_id: The HIT type ID, as provided by mturk.
        :return: None
        """
        # TODO: implement
        raise NotImplementedError()

    def indicate_task_has_hit_type(self, task_id):
        """
        Sets status.awaiting_hit_type parameter of the task, indicating that it has been added to a HIT type

        :param task_id: The task ID, as a string.
        :return: None
        """
        table = self.conn.table(TASK_TABLE)
        table.put(task_id, {'status:awaiting_hit_type': TRUE})

    def set_task_html(self, task_id, html):
        """
        Stores the task HTML in the database, for future reference.

        :param task_id: The task ID, as a string.
        :param html: The task HTML, as a string. [this might need to be pickled?]
        :return: None
        """
        table = self.conn.table(TASK_TABLE)
        table.put(task_id, {'html:c1': html})

    def register_worker(self, worker_id):
        """
        Registers a new worker to the database.

        NOTES:
            This must be keyed by their Turk ID for things to work properly.

        :param worker_id: A string, the worker ID.
        :return: None.
        """
        _log.info('Registering worker %s' % worker_id)
        table = self.conn.table(WORKER_TABLE)
        if self._table_has_row(table, worker_id):
            _log.warning('User %s already exists, aborting.' % worker_id)
        table.put(worker_id, {'status:passed_practice': FALSE, 'status:is_legacy': FALSE, 'status:is_banned': FALSE,
                              'status:random_seed': str(int((datetime.now()-datetime(2016, 1, 1)).total_seconds()))})

    def register_images(self, image_ids, image_urls, attributes=[]):
        """
        Registers one or more images to the database.

        :param image_ids: A list of strings, the image IDs.
        :param image_urls: A list of strings, the image URLs (in the same order as image_ids).
        :param attributes: The image attributes. This will allow us to run separate experiments on different subsets of
                           the available images. This should be a list of strings, such as "people." They are set to
                           True here.
        :return: None.
        """
        # get the table
        _log.info('Registering %i images.'%(len(image_ids)))
        table = self.conn.table(IMAGE_TABLE)
        b = table.batch()
        if type(attributes) is str:
            attributes = [attributes]
        for iid, iurl in zip(image_ids, image_urls):
            imdict = _get_image_dict(iurl)
            if imdict is None:
                continue
            for attribute in attributes:
                imdict['attributes:%s' % attribute] = TRUE
            b.put(iid, imdict)
        b.send()

    def add_attributes_to_images(self, image_ids, attributes):
        """
        Adds attributes to the requested images.

        :param image_ids: A list of strings, the image IDs.
        :param attributes: The image attributes, as a list. See register_images
        :return: None
        """
        if type(image_ids) is str:
            image_ids = [image_ids]
        if type(attributes) is str:
            attributes = [attributes]
        _log.info('Adding %i attributes to %i images.'%(len(attributes), len(image_ids)))
        table = self.conn.table(IMAGE_TABLE)
        b = table.batch()
        for iid in image_ids:
            up_dict = dict()
            for attribute in attributes:
                up_dict['attributes:%s' % attribute] = TRUE
            b.put(iid, up_dict)
        b.send()

    def activate_images(self, image_ids):
        """
        Activates some number of images, i.e., makes them available for tasks.

        :param image_ids: A list of strings, the image IDs.
        :return: None.
        """
        _log.info('Activating %i images.'%(len(image_ids)))
        table = self.conn.table(IMAGE_TABLE)
        b = table.batch()
        for iid in image_ids:
            if not self._table_has_row(table, iid):
                _log.warning('No data for image %s'%(iid))
                continue
            b.put(iid, {'metadata:is_active': TRUE})
        b.send()

    def activate_n_images(self, n, image_attributes=IMAGE_ATTRIBUTES):
        """
        Activates N new images.

        :param n: The number of new images to activate.
        :param image_attributes: The attributes of the images we are selecting.
        :return: None.
        """
        table = self.conn.table(IMAGE_TABLE)
        scanner = table.scan(columns=['metadata:is_active'],
                             filter=attribute_image_filter(image_attributes, only_active=True))
        to_activate = []
        for row_key, rowData in scanner:
            to_activate.append(row_key)
            if len(to_activate) == n:
                break
        self.activate_images(to_activate)

    def practice_served(self, task_id, worker_id):
        """
        Notes that a practice has been served to a worker.

        :param task_id: The ID of the practice served.
        :param worker_id: The ID of the worker to whom the practice was served.
        :return: None.
        """
        _log.info('Serving practice %s to worker %s' % task_id, worker_id)
        # TODO: Check that worker exists
        table = self.conn.table(WORKER_TABLE)
        table.counter_inc(worker_id, 'stats:num_practices_attempted')
        table.counter_inc(worker_id, 'stats:num_practices_attempted_this_week')

    def task_served(self, task_id, worker_id, hit_id=None, hit_type_id=None, payment=None):
        """
        Notes that a task has been served to a worker.

        :param task_id: The ID of the task served.
        :param worker_id: The ID of the worker to whom the task was served.
        :param hit_id: The MTurk HIT ID, if known.
        :param hit_type_id: The hash of the task attribute and the image attributes, as produced by
                          webserver.mturk.get_hit_type_id
        :param payment: The task payment, if known.
        :return: None.
        """
        _log.info('Serving task %s served to %s'%(task_id, worker_id))
        table = self.conn.table(TASK_TABLE)
        table.put(task_id, _conv_dict_vals({'metadata:worker_id': worker_id, 'metadata:hit_id': hit_id,
                                            'metadata:hit_type_id': hit_type_id, 'metadata:payment': payment,
                                            'status:pending_completion': TRUE, 'status:awaiting_serve': FALSE}))
        table = self.conn.table(WORKER_TABLE)
        # increment the number of incomplete trials for this worker.
        table.counter_inc(worker_id, 'stats:num_incomplete')
        table.counter_inc(worker_id, 'stats:numAttempted')
        table.counter_inc(worker_id, 'stats:numAttemptedThisWeek')

    def worker_demographics(self, worker_id, gender, age, location):
        """
        Sets a worker's demographic information.

        :param worker_id: The ID of the worker.
        :param gender: Worker gender.
        :param age: Worker age range.
        :param location: Worker location.
        :return: None
        """
        _log.info('Saving demographics for worker %s'% worker_id)
        table = self.conn.table(WORKER_TABLE)
        table.put(worker_id, _conv_dict_vals({'demographics:age': age, 'demographics:gender': gender,
                                              'demographics:location': location}))

    def task_finished(self, task_id, worker_id, choices, choice_idxs, reaction_times, hit_id=None, assignment_id=None,
                      hit_type_id=None):
        """
        Notes that a user has completed a task.

        :param task_id: The ID of the task completed.
        :param worker_id: The ID of the worker (from MTurk)
        :param choices: In-order choice sequence as image IDs (empty if no choice made).
        :param choice_idxs: In-order choice index sequence as integers (empty if no choice made).
        :param reaction_times: In-order reaction times, in msec (empty if no choice made).
        :param hit_id: The HIT ID, as provided by MTurk.
        :param assignment_id: The assignment ID, as provided by MTurk.
        :param hit_type_id: The HIT type ID, as provided by MTurk.
        :return: None
        """

        _log.info('Saving complete data for task %s worker %s'%(task_id, worker_id))
        table = self.conn.table(TASK_TABLE)
        # check that we have task information for this task
        if not self._table_has_row(table, task_id):
            # issue a warning, but do not discard the data
            _log.warning('No task data for finished task %s' % task_id)
        # update the data
        table.put(task_id,
                  _conv_dict_vals({'metadata:hit_id': hit_id, 'metadata:assignment_id': assignment_id,
                                   'metadata:hit_type_id': hit_type_id, 'completed_data:choices': dumps(choices),
                                   'completed_data:reaction_times': dumps(reaction_times),
                                   'completed_data:choice_idxs': dumps(choice_idxs),
                                   'status:pending_completion': FALSE, 'status:pending_evaluation': TRUE}))
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
        table = self.conn.table(WORKER_TABLE)
        table.counter_dec(worker_id, 'stats:num_incomplete')
        table.counter_inc(worker_id, 'stats:num_pending_eval')

    def practice_pass(self, task_id):
        """
        Notes a pass of a practice task.

        :param task_id: The ID of the practice to reject.
        :return: None.
        """
        table = self.conn.table(TASK_TABLE)
        # check if the table exists
        if not self._table_has_row(table, task_id):
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
        table = self.conn.table(WORKER_TABLE)
        table.put(worker_id, {'worker_status:passed_practice': '1'})
        # TODO: grant the worker the passed practice qualification from webserver.mturk

    def practice_failure(self, task_id, reason=None):
        """
        Notes a failure of a practice task.

        :param task_id: The ID of the practice to reject.
        :param reason: The reason why the practice was rejected. [def: None]
        :return: None.
        """
        _log.info('Nothing needs to be logged for a practice failure at this time.')

    def accept_task(self, task_id):
        """
        Accepts a completed task, updating the worker, task, image, and win tables.

        :param task_id: The ID of the task to reject.
        :return: None.
        """
        # update task table, get task data
        _log.info('Task %s has been accepted.' % task_id)
        table = self.conn.table(TASK_TABLE)
        task_status = self._get_task_status(task_id)
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
        table = self.conn.table(WORKER_TABLE)
        table.counter_dec(worker_id, 'stats:num_pending_eval')  # decrement pending evaluation count
        table.counter_inc(worker_id, 'stats:num_accepted')  # increment accepted count
        # update images table
        table = self.conn.table(IMAGE_TABLE)
        # unfortunately, happybase does not support batch incrementation (arg!)
        choices = loads(task_data.get('completed_data:choices', None))
        for img in choices:
            table.counter_inc(img, 'stats:num_wins')
        # update the win matrix table
        table = self.conn.table(WIN_TABLE)
        b = table.batch()
        img_tuples = task_data.get('metadata:tuples', None)
        img_tuple_types = task_data.get('metadata:tuple_types', None)
        worker_id = task_data.get('metadata:worker_id', None)
        attribute = task_data.get('metadata:attribute', None)
        # iterate over all the values, and store the data in the win table -- as a batch
        ids_to_inc = []  # this will store all the ids that we have to increment (which cant be incremented in a batch)
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

    def reject_task(self, task_id, reason=None):
        """
        Rejects a completed task.

        :param task_id: The ID of the task to reject.
        :param reason: The reason why the task was rejected. [def: None]
        :return: None.
        """
        # fortunately, not much needs to be done for this.
        # update task table, get task data
        _log.info('Task %s has been rejected.' % task_id)
        table = self.conn.table(TASK_TABLE)
        task_status = self._get_task_status(task_id)
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
        table = self.conn.table(WORKER_TABLE)
        table.counter_dec(worker_id, 'stats:num_pending_eval')  # decrement pending evaluation count
        table.counter_inc(worker_id, 'stats:num_rejected')  # increment rejected count
        table.counter_inc(worker_id, 'stats:num_rejected_this_week')

    def reset_worker_counts(self):
        """
        Resets the weekly counters back to 0.

        :return: None.
        """
        table = self.conn.table(WORKER_TABLE)
        scanner = table.scan(filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
        for row_key, data in scanner:
            table.counter_set(row_key, 'stats:num_practices_attempted_this_week', value=0)
            table.counter_set(row_key, 'stats:num_attempted_this_week', value=0)
            table.counter_set(row_key, 'stats:num_rejected_this_week', value=0)

    def ban_worker(self, worker_id, duration=DEFAULT_BAN_LENGTH, reason=DEFAULT_BAN_REASON):
        """
        Bans a worker for some amount of time.

        :param worker_id: The worker ID, as a string.
        :param duration: The amount of time to ban the worker for, in seconds [default: 1 week]
        :param reason: The reason for the ban.
        :return: None.
        """
        table = self.conn.table(WORKER_TABLE)
        table.put(worker_id, _conv_dict_vals({'status:is_banned': TRUE, 'status:ban_duration': duration,
                                              'status:ban_reason': reason}))

    def worker_ban_expires_in(self, worker_id):
        """
        Checks whether or not a worker's ban has expired; if so, it changes the ban status and returns 0. Otherwise, it
        returns the amount of time left in the ban.

        :param worker_id: The worker ID, as a string.
        :return: 0 if the subject is not or is no longer banned, otherwise returns the time until the ban expires.
        """
        table = self.conn.table(WORKER_TABLE)
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

    def reset_timed_out_tasks(self):
        """
        Checks if a task has been pending for too long without completion; if so, it resets it.

        :return: None
        """
        table = self.conn.table(TASK_TABLE)
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

    def deactivate_images(self, image_ids):
        """
        Deactivates a list of images.

        :param image_ids: A list of strings, the image IDs.
        :return: None
        """
        # TODO: Implement
        raise NotImplementedError()

    def deactivate_hit_type(self, hit_type_id):
        """
        Deactivates a hit type ID, so that no new tasks or HITs should be created that are attached to it.

        :param hit_type_id: The HIT type ID, as a string.
        :return: None
        """
        # TODO: Implement
        raise NotImplementedError()