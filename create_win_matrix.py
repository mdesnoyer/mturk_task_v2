# let's figure out how connected the graph is so far
from conf import *
from db import Get
from db import Set
import happybase
import logging
import mturk
import os
import boto
import numpy as np
import scipy as sp

conn = happybase.Connection(DATABASE_LOCATION)
t = conn.table(WIN_TABLE)

ims = []
imss = set()

A = []
B = []
C = []

for n, (cid, i) in enumerate(t.scan()):
    A.append(i['data:winner_id'])
    B.append(i['data:loser_id'])
    C.append(t.counter_get(cid, 'data:win_count'))
    if not (n % 1000):
        print n
