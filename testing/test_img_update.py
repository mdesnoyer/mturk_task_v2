from conf import *
from db import Get
from db import Set
import happybase
from add_new_images import update
import locale


# this has to be run by importing it:
#
# from testing import test_img_update
#
# and it'll run fine.

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)

dbset = Set(pool)
dbget = Get(pool)

update(dbset, dbget, dry_run=False)