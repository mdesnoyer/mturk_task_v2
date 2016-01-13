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

table = conn.table(IMAGE_TABLE)
x = table.scan(filter=b'KeyOnlyFilter() AND FirstKeyOnlyFilter()')
tot_ims = 0
for key, d in x:
    tot_ims += 1

_log.info('Found %i images in database' % tot_ims)
conn.close()