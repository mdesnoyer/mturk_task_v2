"""
Configuration file. Exports relevant information about the database configuration.

Note:
    - Column families are written user under_scores
    - Individual columns are written using camelCase
"""


# OPTIONS
STORE_PRACTICE_PAIRS = False  # if True, will store all practice pairs as if they were in real trials.

# TASK STATUSES
DOES_NOT_EXIST = '-2'  # the task does not exist
UNKNOWN_STATUS = '-1'  # the task exists, but has an unknown status
NOT_BEGUN = '0'  # the task has not been begun
IS_PRACTICE = '1'  # the task is a practice
COMPLETION_PENDING = '2'  # waiting on the worker to complete the task
EVALUATION_PENDING = '3'  # waiting on the task to be evaluated
ACCEPTED = '4'  # the task has been accepted
REJECTED = '5'  # the task has been rejected


# TABLE NAMES
WORKER_TABLE = 'workers'
TASK_TABLE = 'tasks'
IMAGE_TABLE = 'images'
PAIR_TABLE = 'pairs'
WIN_TABLE = 'wins'

# COLUMN NAMES, BY FAMILY
WORKER_FAMILIES = {'status': dict(max_versions=1),
                   'stats': dict(max_versions=1),
                   'demographics': dict(max_versions=1),
                   'attempted_practices': dict(max_versions=1)}
TASK_FAMILIES = {'metadata': dict(max_versions=1),
                 'status': dict(max_versions=1),
                 'completed_data': dict(max_versions=1),
                 'forbidden_workers': dict(max_versions=1)}
IMAGE_FAMILIES = {'metadata': dict(max_versions=1),
                  'stats': dict(max_versions=1),
                  'phash': dict(max_versions=1),
                  'colorname': dict(max_versions=1)}
PAIR_FAMILIES = {'metadata': dict(max_versions=1),
                 'legacy_trials': dict(max_versions=1),
                 'legacy_workers': dict(max_versions=1)}
WIN_FAMILIES = {'data': dict(max_versions=1)}

# DATA INPUT FORMATS
TRUE = '1'
FALSE = '0'
FLOAT_STR = '%.4g'  # how to format strings / integers