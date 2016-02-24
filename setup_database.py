"""
This script sets up the databases that are required the webapp, and adds in
all the image information.

This is designed to run on the instance that's managing the HBase database
that's supporting the webapp--thus the database location is localhost.

Note, the database must be running:
    $HBASE_HOME/bin/start-hbase.sh

"""

from conf import *
from itertools import izip_longest
from db import Get
from db import Set
import happybase
from glob import glob
import logging
import boto3
import botocore
import sys


def gen_url(obj):
    """
    Creates a URL for an S3 object.

    NOTES:
        This will only work for public-read stuff.

    :param obj: An S3 object as yielded from boto3.
    :return: A URL for this object.
    """
    url = 'https://s3.amazonaws.com/{bucket}/{key}'
    url = url.format(bucket=obj.bucket_name, key=obj.key)
    return url


def is_public_read(obj):
    """
    Determines if an object is public read or not.

    :param obj: An S3 object as yielded from boto3.
    :return: True if the object is public-read, False otherwise.
    """
    for grant in obj.Acl().grants:
        if 'URI' in grant['Grantee']:
            if (grant['Grantee']['URI'] ==
                    'http://acs.amazonaws.com/groups/global/AllUsers'):
                if grant['Permission'] == 'READ':
                    return True
    return False


def grouper(iterable, n, fillvalue=None):
    """
    Collect data into fixed-length chunks or blocks.

    :param iterable: An object supporting iteration.
    :param n: The size of each chunk.
    :param fillvalue: The fill for unevenly sized chunks.

    :return: The iterator.
    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return izip_longest(fillvalue=fillvalue, *args)

logger.config_root_logger('/data/logs/scratch_setup.log')
_log = logger.setup_logger('database_setup')
_log.setLevel(logging.DEBUG)

_log.info('Connecting to S3')
s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_ID,
                    aws_secret_access_key=AWS_SECRET_KEY)

_log.info('Checking for bucket %s' % IMAGE_BUCKET)
bucket = s3.Bucket(IMAGE_BUCKET)
exists = True
try:
    s3.meta.client.head_bucket(Bucket=IMAGE_BUCKET)
except botocore.exceptions.ClientError as e:
    # If a client error is thrown, then check that it was a 404 error.
    # If it was a 404 error, then the bucket does not exist.
    error_code = int(e.response['Error']['Code'])
    if error_code == 404:
        exists = False

if exists:
    _log.info('...bucket found')
else:
    _log.critical('..bucket not found!')
    _log.info('Nothing to be done; existing')
    sys.exit()

_log.info('Connecting to database')
conn = happybase.Connection('localhost')

_log.info('Instantiating db connections')
dbget = Get(conn)
dbset = Set(conn)

_log.info('Building tables')
dbset.force_regen_tables()

tot = 0
for keys in grouper(bucket.objects.all(), 1000, None):
    tot += len(keys)
    _log.debug('%i images fetched so far' % tot)
    # ensure the image is public-read
    imgids = []
    imgs = []
    for key in keys:
        if key is None:
            continue
        key.Acl().put(ACL='public-read')
        img = gen_url(key)
        imgid = img.split('/')[-1].split('.')[0]
        imgids.append(imgid)
        imgs.append(img)
    dbset.register_images(imgids, imgs, attributes='original')

