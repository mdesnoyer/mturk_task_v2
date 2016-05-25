"""
This will instantiate a dbget object and attempt to build an instance of the
new sampler object, then characterize its behavior.
"""
from conf import *
from db import Get
import happybase
import time

_log = logger.setup_logger(__name__)

pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)
dbget = Get(pool)

print 'Setting up the sampler object'
dbget.update_sampling()
