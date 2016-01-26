"""
GLOBAL UTILITY FUNCTIONS

Warning: This cannot be used in isolation (right now), mostly because I  haven't had time to fix up all the imports.
"""

# TODO: Fix imports

import numpy as np
from itertools import combinations as comb
import random
import string
import cStringIO
import urllib
import numpy as np
from itertools import combinations as comb


"""
ID GENERATION
"""
_id_len = 16


def _rand_id_gen(n):
    """
    Generates random IDs
    :param n: The number of characters in the random ID
    :return: A raw ID string, composed of n upper- and lowercase letters as well as digits.
    """
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(n))


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
    return PREFIX_PREFIX + _rand_id_gen(_id_len)


"""
GET utils (for db.get)
"""
def pair_to_tuple(image1, image2):
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


def get_im_dims(imageUrl):
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


def attribute_image_filter(attributes=[], filter_type=ANY, only_active=False, only_inactive=False):
    """
    Returns a filter appropriate to HBase / HappyBase that will find images based on a list of their attributes and
    (optionally) whether or not they are active.

    :param attributes: A list of attributes as strings.
    :param filter_type: Whether all columns are required or at least one column is required. By default, having any of
                        the required columns is sufficient.
    :param only_active: A boolean. If true, will find only active images.
    :param only_inactive: A boolean. If true, will find only inactive images.
    :return: An image filter, as a string.
    """
    if only_active and only_inactive:
        raise ValueError('Cannot filter for images that are both active and inactive')
    if filter_type is not ALL and filter_type is not ANY:
        raise ValueError('Filter types may either be ANY or ALL')
    f = [column_boolean_filter('attributes', attribute, TRUE) for attribute in attributes]
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
    f = "SingleColumnValueFilter ('%s', '%s', =, 'regexstring:^%s$', true, true)"
    f = f % (column_family, column_name, str(value))
    return f


def general_filter(column_tuples, values, filter_type=ALL, key_only=False):
    """
    General filter for tables, creating a filter that returns rows that satisfy the specified requirements.

    :param column_tuples: A list of column tuples of the form [[column family 1, column name 1], ...]
    :param values: A list of values that the columns should have, in-order.
    :param filter_type: Either ALL or ANY. If ALL, all the column values must be satisfied. If ANY, at least one column
                        value match must be met.
    :param key_only: The filter will only return row keys and not the entire rows.
    :return: The appropriate filter under the specification.
    """
    if filter_type is not ALL and filter_type is not ANY:
        raise ValueError('Filter types may either be ANY or ALL')
    f = [_column_boolean_filter(x, y, v) for ((x, y), z) in zip(column_tuples, values)]
    f = (' ' + filter_type.strip() + ' ').join(f)
    if key_only:
        f += ' AND KeyOnlyFilter() AND FirstKeyOnlyFilter()'
    return f


"""
TASK GENERATION UTILS
"""


def get_design(n, t, j):
    """
    Creates an experimental design by creating a series of fixed-length subsets of N elements such that each element
    appears at least some number of times and no pairs of elements occurs in any subset more than once. The number of
    subsets is minimized. Each subset can be appropriately conceptualized as a "trial."

    This constitutes an incomplete t-Design. It effectively extends t-Designs to a new type of design,
    t_min-(v, k, lambda, x), which is an incidence structure such that:
        - (1) There are v points.
        - (2) Each block contains k points.
        - (3) For any t points there are exactly lambda blocks that contain all these points.
        - (4) Each point occurs in at least x blocks.
        - (5) No block can be removed without violating 1-4, i.e., it is 'minimal' in a sense.

    See:
        --- The general format is a T-design: http://mathworld.wolfram.com/t-Design.html
        --- In our case, because pairs must occur precisely once, it is a Steiner System:
            http://mathworld.wolfram.com/SteinerSystem.html

    If no such design is possible, returns None.

    This generates a Steiner system deterministically--due to the hypergeometric rate of expansion, it is not possible
    to generate unique Steiner systems each time without using a random component, which makes things so much messier.
    I may introduce such a method later on. It is not clear if this determinism will introduce a bias in the responses.
    My guess is that, under random assignment of images to indices, no bias is possible--but I'm not sure if thats true.

    :param n: The number of distinct elements involved in the experiment.
    :param t: The number of elements to present each trial.
    :param j: The number of times each element should appear during the experiment.
    :return: A list of tuples representing each subset. Elements may be randomized within element and subset order may
    be randomized without consequence.
    """
    obs = np.zeros((n, n)) # pair observation matrix
    occ = np.zeros(n) # counter for the number of observations
    combs = [] # the combinations that will be returned.
    for allvio in range(t): # minimize the number of j-violations (i.e., elements appearing more than j-times)
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