"""
Exports utilities for use in get and set. Additionally, thought unintentionally, this also imports everything defined in
conf to the get and set namespace.
"""


import urllib
import cStringIO
from PIL import Image


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