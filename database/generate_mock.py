"""
Rebuilds the database, and inserts some mock data, from AVA
"""
from glob import glob
import numpy as np
import happybase
from database import set as dbset
from database import get as dbget
from database.conf import *
from database import logger
import ipdb
#import logger
import logging

_log = logger.setup_logger('generate_mock')
_log.setLevel(logging.DEBUG)

_log.info('Connecting to database')
conn = happybase.Connection('localhost')
_log.info('Rebuilding tables')
dbset.force_regen_tables(conn)


n_images = 300

images = glob('/data/AVA/images/*.jpeg')
_log.info('Found %i images' % len(images))
_log.info('Selecting %i to be input into the database' % n_images)

ims = list(np.random.choice(images, n_images, replace=False))

im_ids = [x.split('/')[-1].split('.')[0] for x in ims] # get the image ids
_log.info('Registering images...')
dbset.register_images(conn, im_ids, ims)
_log.info('Activating first 100 images...')
dbset.activate_images(conn, im_ids[:100])

table = conn.table(IMAGE_TABLE)

total_count = dbget.get_num_items(table)
active_count = dbget.get_n_active_images(conn=conn)

_log.info('Found %i images total in database' % total_count)
_log.info('Found %i active images in database' % active_count)

#scanner = table.scan(columns=['stats:numTimesSeen', 'metadata:isActive'], filter=dbget.ACTIVE_FILTER)

_log.info('Attempting to generate design')
design = dbget.get_design(conn, 20, 3, 1)

print design

conn.close()