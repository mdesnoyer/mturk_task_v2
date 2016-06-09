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

with open('/Users/ndufour/Desktop/mturk_data/old_data', 'r') as f:
    for line in f:
        win, lose, gender, age_b, wid = line.strip().split(',')
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
            bs = locale.format("%d", bad, grouping=True)
            print '%s records collected (%s bad)' % (v2, bs)

with open('/Users/ndufour/Desktop/mturk_data/new_data', 'r') as f:
    for line in f:
        win, lose, gender, age, wid = line.strip().split(',')
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
            bs = locale.format("%d", bad, grouping=True)
            print '%s records collected (%s bad)' % (v2, bs)

with open('/Users/ndufour/Desktop/mturk_data/combined', 'w') as f:
    for n, k in enumerate(sorted(data.keys())):
        if not n % 10000:
            v2 = locale.format("%d", n, grouping=True)
            print '%s records written' % v2
        a, b = k
        winstr = ','.join([str(x) for x in data[k]])
        f.write('%s,%s,%s\n' % (a, b, winstr))
