from conf import *
from db import Get
from db import Set
import happybase
from add_new_images import update

pool = happybase.ConnectionPool(size=2, host=DATABASE_LOCATION)

dbset = Set(pool)
dbget = Get(pool)

update(dbset, dbget, dry_run=True)