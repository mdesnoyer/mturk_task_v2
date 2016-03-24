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

L = list(set(A + B))
N = len(L)

bA = {a:L.index(a) for a in set(A)}
bB = {b:L.index(b) for b in set(B)}
M = sp.sparse.lil_matrix((N, N), np.uint32)
for n, (a, b, c) in enumerate(zip(A, B, C)):
    print n
    M[bA[a], bB[b]] = c

n, labels = sp.sparse.csgraph.connected_components(M, directed=True,
                                               connection='strong')
print 'Obtained %i components' % n

ccount = np.zeros(n)
for l in labels:
    ccount[l] += 1