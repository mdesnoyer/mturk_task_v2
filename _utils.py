"""
GLOBAL UTILITY FUNCTIONS

Warning: This cannot be used in isolation (right now), mostly because I
haven't had time to fix up all the imports, and this requires global
variables defined in conf.
"""

import random
import string
import cStringIO
import urllib
from itertools import combinations as comb
import logger
from PIL import Image
from _globals import *
import re
import socket
import subprocess
import time

_log = logger.setup_logger(__name__)


"""
GENERAL
"""


_first_cap_re = re.compile('(.)([A-Z][a-z]+)')
_all_cap_re = re.compile('([a-z0-9])([A-Z])')


def convert(name):
    """
    Converts a camelCase string to camel_case. I only needed to use this once
    for a README document about the database schema; however, I liked it so
    much I want to save it.

    NOTES:
        source: http://stackoverflow.com/questions/1175208/
        elegant-python-function-to-convert-camelcase-to-camel-case

    :param name: A string.
    :return: name, only with camelCase replaced with camel_case.
    """
    s1 = _first_cap_re.sub(r'\1_\2', name)
    return _all_cap_re.sub(r'\1_\2', s1).lower()


def chunks(l, k):
    """
    Yields k approximately uniformly sized lists from l

    :param l: A list
    :param k: The total number of sublists to yield.
    :return: Iterator over sublists on the list l.
    """
    if k < 1:
        yield []
        raise StopIteration
    n = len(l)
    avg = n/k
    remainders = n % k
    start, end = 0, avg
    while start < n:
        if remainders > 0:
            end = end + 1
            remainders = remainders - 1
        yield l[start:end]
        start, end = end, end+avg


"""
ID GENERATION
"""
_id_len = 16


def _rand_id_gen(n):
    """
    Generates random IDs
    :param n: The number of characters in the random ID
    :return: A raw ID string, composed of n upper- and lowercase letters
    as well as digits.
    """
    return ''.join(random.choice(string.ascii_uppercase +
                                 string.ascii_lowercase +
                                 string.digits) for _ in range(n))


def rand_id_gen(n):
    return _rand_id_gen(n)


def task_id_gen():
    """
    Generates task IDs
    :return: A task ID, as a string
    """
    return TASK_PREFIX + _rand_id_gen(_id_len)


def practice_id_gen():
    """
    Generates practice IDs
    :return: A practice ID, as a string
    """
    return PRACTICE_PREFIX + _rand_id_gen(_id_len)


"""
GET utils (for db.get)
"""


def counter_str_to_int(cstring):
    """
    Converts the string representation of a counter to an integer value.

    :param cstring: The string representing the value of a counter,
    for instance the value of a table.row(...) call under the field
    representing a counter.
    :return: An integer, the value of the counter.
    """
    tot = 0
    for i in cstring:
        tot = (tot << 8) + ord(i)
    return tot


def pair_to_tuple(image1, image2):
    """
    Converts an image pair into a sorted tuple.

    :param image1: An image ID (ordering irrelevant)
    :param image2: An image ID (ordering irrelevant)
    :return: A sorted image tuple.
    """
    if image1 > image2:
        return image2, image1
    else:
        return image1, image2


def get_im_dims(imageUrl):
    """
    Returns the dimensions of an image file in pixels as [width, height].
    This is unfortunately somewhat time consuming as the images have to be
    loaded in order to determine their dimensions. Its likely that this can
    be accomplished in pure javascript, however I (a) don't know enough
    javascript and (b) want more explicit control.

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


def attribute_image_filter(attributes=[], filter_type=None, only_active=False,
                           only_inactive=False):
    """
    Returns a filter appropriate to HBase / HappyBase that will find images
    based on a list of their attributes and (optionally) whether or not they
    are active.

    :param attributes: A list of attributes as strings.
    :param filter_type: Whether all columns are required or at least one column
                        is required. By default, having any of the required
                        columns is sufficient. [default: ANY]
    :param only_active: A boolean. If true, will find only active images.
                        [default: False]
    :param only_inactive: A boolean. If true, will find only inactive images.
                          [default: False]
    :return: An image filter, as a string.
    """
    if filter_type is None:
        filter_type = ANY
    if only_active and only_inactive:
        raise ValueError('Cannot filter for images that are both active '
                         'and inactive')
    if filter_type is not ALL and filter_type is not ANY:
        raise ValueError('Filter types may either be ANY or ALL')
    if only_active and not len(attributes):
        return ACTIVE_FILTER
    if only_inactive and not len(attributes):
        return INACTIVE_FILTER
    f = [column_boolean_filter('attributes', attribute, TRUE) for
         attribute in attributes]
    f = (' ' + filter_type.strip() + ' ').join(f)
    if only_active:
        f = '(' + ACTIVE_FILTER + ')' + ' AND ' + '(' + f + ')'
    elif only_inactive:
        f = '(' + INACTIVE_FILTER + ')' + ' AND ' + '(' + f + ')'
    return f


def column_boolean_filter(column_family, column_name, value):
    """
    Creates a generic single column filter returns when column is true.

    :param column_family: The HBase / HappyBase column family
    :param column_name: The HBase / Happybase column family
    :param value: The required value for that column.
    :return: The filter, as a string.
    """
    f = "SingleColumnValueFilter ('%s', '%s', =, 'regexstring:^%s$', true, " \
        "true)"
    f = f % (column_family, column_name, str(value))
    return f


def general_filter(column_tuples, values, filter_type=None, key_only=False):
    """
    General filter for tables, creating a filter that returns rows that satisfy
    the specified requirements.

    :param column_tuples: A list of column tuples of the form
                         [[column family 1, column name 1], ...]
    :param values: A list of values that the columns should have, in-order.
    :param filter_type: Either ALL or ANY. If ALL, all the column values must
                        be satisfied. If ANY, at least one column value match
                        must be met. [default: ALL]
    :param key_only: The filter will only return row keys and not the entire
                     rows. [default: False]
    :return: The appropriate filter under the specification.
    """
    if filter_type is None:
        filter_type = ALL
    if filter_type is not ALL and filter_type is not ANY:
        raise ValueError('Filter types may either be ANY or ALL')
    f = [column_boolean_filter(x, y, v) for ((x, y), v) in
         zip(column_tuples, values)]
    f = (' ' + filter_type.strip() + ' ').join(f)
    if key_only:
        f += ' AND KeyOnlyFilter() AND FirstKeyOnlyFilter()'
    return f


"""
TASK GENERATION UTILS
"""


def get_design(n, t, j):
    """
    Creates an experimental design by creating a series of fixed-length
    subsets of N elements such that each element appears at least some number
    of times and no pairs of elements occurs in any subset more than once. The
    number of subsets is minimized. Each subset can be appropriately
    conceptualized as a "trial."

    This constitutes an incomplete t-Design. It effectively extends t-Designs
    to a new type of design, t_min-(v, k, lambda, x), which is an incidence
    structure such that:
        - (1) There are v points.
        - (2) Each block contains k points.
        - (3) For any t points there are exactly lambda blocks that contain
              all these points.
        - (4) Each point occurs in at least x blocks.
        - (5) No block can be removed without violating 1-4, i.e., it is
              'minimal' in a sense.

    See:
        --- The general format is a T-design:
                http://mathworld.wolfram.com/t-Design.html
        --- In our case, because pairs must occur precisely once, it is a
            Steiner System:
                http://mathworld.wolfram.com/SteinerSystem.html

    If no such design is possible, returns None.

    This generates a Steiner system deterministically--due to the
    hypergeometric rate of expansion, it is not possible to generate unique
    Steiner systems each time without using a random component, which makes
    things so much messier. I may introduce such a method later on. It is not
    clear if this determinism will introduce a bias in the responses. My guess
    is that, under random assignment of images to indices, no bias is
    possible--but I'm not sure if thats true.

    :param n: The number of distinct elements involved in the experiment.
    :param t: The number of elements to present each trial.
    :param j: The number of times each element should appear during the
              experiment.
    :return: A list of tuples representing each subset. Elements may be
             randomized within element and subset order may
    be randomized without consequence.
    """
    obs = np.zeros((n, n)) # pair observation matrix
    occ = np.zeros(n) # counter for the number of observations
    combs = [] # the combinations that will be returned.
    # minimize the number of j-violations (i.e., elements appearing more than
    # j-times)
    for allvio in range(t):
        for c in comb(range(n), t):
            if np.min(occ) == j:
                return combs # you're done
            cvio = 0 # the current count of violations
            for x1, x2 in comb(c, 2):
                if obs[x1, x2]:
                    continue
            for i in c:
                cvio += max(0, occ[i] - j + 1)
            if cvio > allvio:
                continue
            for x1, x2 in comb(c, 2):
                obs[x1, x2] += 1
            for i in c:
                occ[i] += 1
            combs.append(c)
    if not np.min(occ) >= j:
        return None
    return combs

"""
TESTING UTILITIES
"""


def establish_tunnel(local_port=9000,
                     db_ip=DATABASE_LOCATION,
                     db_port=9090,
                     ssh_key='mturk_stack_access.pem'):
    """
    Establishes an ssh tunnel to the database or instance of
    your choice.

    :param local_port: The local port you want to forward.
    :param db_ip: The database / instance IP address.
    :param db_port: The port on the instance to forward to.
    :param ssh_key: The ssh key to use.
    :return: The subprocess representing the connection.
    """
    cmd = ('ssh -i ~/.ssh/{ssh_key} -L {local}:localhost:{db_port} ubuntu@{'
           'db_addr}').format(
        ssh_key=ssh_key,
        local=local_port,
        db_port=db_port,
        db_addr=db_ip)
    _log.info('Forwarding port %s to the database' % local_port)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT,
                            stdin=subprocess.PIPE)
    # Wait until we can connect to the db
    sock = None
    for i in range(12):
        try:
            sock = socket.create_connection(('localhost', local_port), 3600)
        except socket.error:
            time.sleep(5)
    if sock is None:
        raise Exception('Could not connect to the database')

    _log.info('Connection made to the database')
    sock.shutdown(socket.SHUT_RDWR)
    sock.close()

    # ensure that proc is terminated with proc.terminate() before you exit!
    return proc
