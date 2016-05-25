"""
This is a utility function that will simply create a field
within the completion data that includes the total time in seconds.

At least, that was its original intent. However, it looks like the original
get_mean_times function was extremely inefficient. so I updated it, and this
might be sufficient on its own.
"""

from conf import *
from db import Get
import happybase
import time

_log = logger.setup_logger(__name__)

pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)
dbget = Get(pool)

print 'new-er version...'

avg_meth_new = dbget._get_mean_task_time_old()

print 'newest version...'

avg_meth_newest = dbget._get_mean_task_time()

print 'newer:',avg_meth_new,'newest:',avg_meth_newest

