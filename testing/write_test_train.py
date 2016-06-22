# assemble the training set.
# 0 = <= 29
# 1 = 30 - 39
# 2 = 40 - 49
# 3 = 50+
# 4 = Unk

# male = 0
# female = 5

# 0 = male / <= 29
# 1 = male / 30 - 39
# 2 = male / 40 - 49
# 3 = male / 50+
# 4 = female / <= 29
# 5 = female / 30 - 39
# 6 = female / 40 - 49
# 7 = female / 50+
# 8 = Unk / Unk

# bins = Counter()
# for i in f:
#     a, b, c, d, e = i.split(',')
#     bins[d] += 1

# for i in f:
#     a, b, c, d, e = i.split(',')
#     bins[c] += 1

# i.e.,
# (a, b) = < # of times a beat b by demo> < # of times b beat a by demo >

from collections import defaultdict as ddict
# import networkx as nx
import locale
import sys

try:
    # for linux
    locale.setlocale(locale.LC_ALL, 'en_US.utf8')
except:
    # for mac
    locale.setlocale(locale.LC_ALL, 'en_US')

test_size = 1000  # the number of EDGES in the testing set, such that at
# least one image in each pair (i.e., edge) is in the testing set. What we
# could do is greedily add images, by selecting the images with the minimum
# number of edges.

# first step is to determine all the available images that we've got
all_ims = dict()
with open('/data/aquila_data/avail_images/imagelist', 'r') as f:
    for line in f:
        all_ims[line.strip().split('/')[-1].split('.')[0]] = \
            line.strip().split('/')[-1]

# find unconnected components, so create a train and a test set.
# to do this, represent it as an undirected graph and look for disconnected
# components.

# G = nx.Graph()
#
# with open('/data/aquila_data/mturk_task_v1/data', 'r') as f:
#     for n, line in enumerate(f):
#         win, lose, gender, age_b, wid = line.strip().split(',')
#         if win.split('.')[0] in all_ims and lose.split('.')[0] in all_ims:
#             G.add_edge(win.split('.')[0], lose.split('.')[0])
#         else:
#             continue
#         if not n % 10000:
#             psz = locale.format("%d", n, grouping=True)
#             print 'Building Graph (from v1) %s processed' % (psz)
# print 'Done with V2, graph size is %s' % locale.format("%d", G.size(), grouping=True)
#
# with open('/data/aquila_data/mturk_task_v2/data', 'r') as f:
#     for n, line in enumerate(f):
#         win, lose, gender, age, wid, tid = line.strip().split(',')
#         if win in all_ims and lose in all_ims:
#             G.add_edge(win, lose)
#         else:
#             continue
#         if not n % 10000:
#             psz = locale.format("%d", n, grouping=True)
#             print 'Building Graph (from v2) %s processed' % (psz)
# print 'Done with V2, graph size is %s' % locale.format("%d", G.size(), grouping=True)
#
# # let's try to make a test set that will minimize the amount of data discarded.
# # select a node that has the least number of edges
# edges = set()
# nodes = []


# this maps the age brackets to an 'age' that will be useful later
old_data_age_map = {'': None, '0-19': 25,
                    '18-29': 25, '20-29': 25, '30-39': 35,
                    '40-49': 45, '50-59': 55, '60-69': 55,
                    '70+': 55, 'None': None}

male_str = 'male'
fem_str = 'female'

import locale

try:
    # for linux
    locale.setlocale(locale.LC_ALL, 'en_US.utf8')
except:
    # for mac
    locale.setlocale(locale.LC_ALL, 'en_US')


def get_demo(age, gender):
    # returns their bracket; should work on both data types.
    if age is None or gender is None:
        return 8

    if gender == male_str:
        gend_v = 0
    elif gender == fem_str:
        gend_v = 4
    else:
        return 8

    age = int(age)
    if age < 30:
        age_v = 0
    elif age < 40:
        age_v = 1
    elif age < 50:
        age_v = 2
    else:
        age_v = 3
    return age_v + gend_v

data = ddict(lambda: [0] * 18)

tot = 0
bad = 0
bad_ims_v1 = set()
bad_ims_v2 = set()
good_im_cnt = 0

with open('/data/aquila_data/mturk_task_v1/data', 'r') as f:
    for line in f:
        win, lose, gender, age_b, wid = line.strip().split(',')
        win = win.split('.')[0]
        lose = lose.split('.')[0]
        if win not in all_ims:
            bad_ims_v1.add(win)
            continue
        if lose not in all_ims:
            bad_ims_v1.add(lose)
            continue
        good_im_cnt += 2
        if not win or not lose:
            # some records have only a win or a lose, not both. arg.
            bad += 1
            continue
        if win < lose:
            key = (win, lose)
            base_v = 0
        elif win > lose:
            key = (lose, win)
            base_v = 9  # offset the bin by 8 because it's a loss
        age = old_data_age_map[age_b]
        demo_idx = get_demo(age, gender)
        data[key][demo_idx + base_v] += 1
        tot += 1
        if not tot % 10000:
            v2 = locale.format("%d", tot, grouping=True)
            bs = locale.format("%d", bad + len(bad_ims_v1), grouping=True)
            print '[v1] %s records collected (%s bad)' % (v2, bs)
tot_old = len(data)

with open('/data/aquila_data/mturk_task_v2/data', 'r') as f:
    for line in f:
        win, lose, gender, age, wid, tid = line.strip().split(',')
        if win.split('.')[0] not in all_ims:
            bad_ims_v2.add(win)
            continue
        if lose.split('.')[0] not in all_ims:
            bad_ims_v2.add(lose)
            continue
        good_im_cnt += 2
        if not win or not lose:
            bad += 1
            continue
        if win < lose:
            key = (win, lose)
            base_v = 0
        elif win > lose:
            key = (lose, win)
            base_v = 8  # offset the bin by 8 because it's a loss
        demo_idx = get_demo(age, gender)
        data[key][demo_idx + base_v] += 1
        tot += 1
        if not tot % 10000:
            v2 = locale.format("%d", tot, grouping=True)
            bs = locale.format("%d", bad + len(bad_ims_v2), grouping=True)
            print '[v2] %s records collected (%s bad)' % (v2, bs)

tot_new = len(data) - tot_old


with open('/data/aquila_data/combined', 'w') as f:
    for n, k in enumerate(sorted(data.keys())):
        if not n % 10000:
            v2 = locale.format("%d", n, grouping=True)
            print '%s records written' % v2
        a, b = k
        winstr = ','.join([str(x) for x in data[k]])
        f.write('%s,%s,%s\n' % (all_ims[a], all_ims[b], winstr))

with open('/data/aquila_data/bad_v1', 'w') as f:
    f.write('\n'.join(bad_ims_v1))

with open('/data/aquila_data/bad_v2', 'w') as f:
    f.write('\n'.join(bad_ims_v2))

v1o = locale.format("%d", tot_old, grouping=True)
v2o = locale.format("%d", tot_new, grouping=True)

v2 = locale.format("%d", n, grouping=True)
print '%s obtained from v1, %s obtained from v2' % (v1o, v2o)
print '%s records written' % v2
print 'All files written'