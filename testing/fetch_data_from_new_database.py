"""
Fetches new data (in raw form) from mturk task v2
"""
from conf import *
import happybase
from dill import loads
import locale

proc = establish_tunnel()


try:
    # for linux
    locale.setlocale(locale.LC_ALL, 'en_US.utf8')
except:
    # for mac
    locale.setlocale(locale.LC_ALL, 'en_US')


def get_age(birthyear):
    """
    Returns the age given the output of the birthyear column.

    :param birthyear: The value of birthyear in the worker demo.
    :return: The age, or none.
    """
    if birthyear is None:
        return None
    # prune out the concatenation errors (i.e., 1901992)
    birthyear = birthyear[-4:]
    try:
        birthyear = int(birthyear)
    except:
        return None
    if birthyear < 110:
        # then they put in their age, arg.
        return birthyear
    else:
        return 2016 - birthyear

# step 1: get all the worker demographics
worker_data = dict()
conn = happybase.Connection(host='localhost', port=9000)
table = conn.table(WORKER_TABLE)
s = table.scan(columns=['demographics:gender', 'demographics:birthyear'])
for n, (wid, data) in enumerate(s):
    if not n % 1000:
        print 'Fetching workers:', n
    age = get_age(data.get('demographics:birthyear', None))
    gender = data.get('demographics:gender', None)
    worker_data[wid] = [age, gender]

# step 2: get a list of tasks
task_list = []
table = conn.table(TASK_TABLE)
#s = table.scan(filter='KeyOnlyFilter() AND FirstKeyOnlyFilter()')
s = table.scan(columns=['status:accepted', 'metadata:is_practice'])
for n, (tid, d) in enumerate(s):
    if not n % 1000:
        print 'Fetching task IDs:', n
    if d.get('status:accepted', FALSE) == TRUE:
        if d.get('metadata:is_practice', FALSE) == FALSE:
            task_list.append(tid)

d2g = ['completion_data:choices', 'metadata:worker_id',
       'completion_data:action', 'metadata:tuples']

keep_key = 'keep'
rej_key = 'reject'
recorded_tids = set()
if os.path.exists('/data/aquila_data/mturk_task_v2/data'):
    with open('/data/aquila_data/mturk_task_v2/data', 'r') as f:
        for line in f:
            _, _, _, _, _, tid = line.strip().split(',')
            recorded_tids.add(tid)

with open('/data/aquila_data/mturk_task_v2/data', 'a') as f:
    for n, tid in enumerate(task_list):
        if not n % 100:
            print 'Fetching data %i / %i, task %s' % (n, len(task_list), tid)
        if tid in recorded_tids:
            continue
        data = table.row(tid, columns=d2g)
        wid = data['metadata:worker_id']
        choices = loads(data['completion_data:choices'])
        tuples = loads(data['metadata:tuples'])
        actions = loads(data['completion_data:action'])
        for choice, (i1, i2, i3), con in zip(choices, tuples, actions):
            if choice == -1:
                continue
            age, gender = worker_data.get(wid, (None, None))
            if con == keep_key:
                tups = [(choice, x) for x in [i1, i2, i3] if x != choice]
            elif con == rej_key:
                tups = [(x, choice) for x in [i1, i2, i3] if x != choice]
            for i, j in tups:
                if gender is None:
                    gend_str = ''
                else:
                    gend_str = gender
                if age is None:
                    age_str = ''
                else:
                    age_str = str(age)
                f.write('%s,%s,%s,%s,%s,%s\n' % (i, j, gend_str, age_str, wid,
                                                 tid))



proc.terminate()
