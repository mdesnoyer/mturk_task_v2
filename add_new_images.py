"""
This is a utility script.

Exports a function that adds all the newly discovered images.

NOTE:
    This will have to be updated if we add more images from a different
    location.
"""

import boto3
from conf import *
import locale

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

def _parse_key(key):
    vals = key.split('.')[0].split('_')
    sim = float(vals[-1]) / 10000
    ttype = vals[-2]
    vid = '_'.join(vals[:-2])
    return vid, sim, ttype


def _yt_filt_func(key):
    _, sim, _ = _parse_key(key)
    return sim < 0.027


def _s3sourcer(bucket_name, filter_func=None):
    """
    Constructs an iterator over the items in a bucket, returning those that
    pass the filter function. The iterator returns image id, url tuples.

    :param bucket_name: The name of the bucket to iterate over.
    :param filter_func: The filter function, which checks if an item should be
    yielded based on its key.
    :return: None.
    """
    if filter_func is None:
        filter_func = lambda x: True
    AWS_ACCESS_ID = 'AKIAIS3LLKRK7HDX4XYA'
    AWS_SECRET_KEY = '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'
    s3 = boto3.resource('s3', aws_access_key_id=AWS_ACCESS_ID,
                        aws_secret_access_key=AWS_SECRET_KEY)
    bucket_iter = iter(s3.Bucket(bucket_name).objects.all())
    base_url = 'https://s3.amazonaws.com/%s/%s'
    while True:
        item = bucket_iter.next()
        if filter_func(item.key):
            image_id = item.key.split('.')[0]
            image_url = base_url % (bucket_name, item.key)
            yield (image_id, image_url, item)
    return


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

def update(dbset, dbget, dry_run=False):
    sources = [_s3sourcer('neon-image-library'),
               _s3sourcer('mturk-youtube-thumbs',
                          filter_func=_yt_filt_func)]
    to_add_ids = []
    to_add_urls = []
    tot = 0
    with dbget.pool.connection() as conn:
        # just use dbget's connection, fuck it.
        table = conn.table(IMAGE_TABLE)
        print 'Fetching known images'
        # we fetch all the images due to how long it takes to check if an image
        # is already in the database. clearly, as the number of images gets
        # very large, we won't be able to keep doing this. but we'll burn
        # that bridge when we get to it, i guess.
        known_ims = set(dbget.get_items(table))
        print 'Searching for new images.'
        for n, source in enumerate(sources):
            for m, (imid, imurl, obj) in enumerate(source):
                if not m % 1000:
                    v1 = locale.format("%d", m, grouping=True)
                    v2 = locale.format("%d", tot, grouping=True)
                    print '%i - %s - %s' % (n, v1, v2)
                if imid in known_ims:
                    known_ims.remove(imid)
                    continue
                if not dry_run:
                    #obj.Acl().put(ACL='public-read')
                    pass
                    # all the 'bad' ones should be changed at this point
                tot += 1
                to_add_ids.append(imid)
                to_add_urls.append(imurl)
                if len(to_add_urls) >= 1000:
                    if not dry_run:
                        print 'Registering %i images' % len(to_add_urls)
                        dbset.register_images(to_add_ids, to_add_urls)
                        to_add_ids = []
                        to_add_urls = []
    num_to_add = locale.format("%d", tot, grouping=True)
    if dry_run:
        print 'Would add %s images, but this is a dry run.' % (num_to_add)
    else:
        print 'Added %s new images' % (num_to_add)
