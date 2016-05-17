from conf import *
from db import Get
from db import Set
import happybase
from add_new_images import update
import locale

locale.setlocale(locale.LC_ALL, 'en_US.utf8')

pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)

dbset = Set(pool)
dbget = Get(pool)

update(dbset, dbget, dry_run=True)