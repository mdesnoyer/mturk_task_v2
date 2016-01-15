"""
Configuration file. Exports relevant information about the database configuration.

Note:
    - Column families are written user under_scores
    - Individual columns are written using camelCase
"""
import numpy as np

# OPTIONS
STORE_PRACTICE_PAIRS = False  # if True, will store all practice pairs as if they were in real trials.
ACTIVATION_CHUNK_SIZE = 500  # The number of images to activate in a chunk.
DEFAULT_BAN_LENGTH = float(60*60*24*7)  # the default length of time for a ban
TASK_COMPLETION_TIMEOUT = float(60*60*24)  # the amount of time a task may be pending without being completed
DEFAULT_BAN_REASON = 'Reason not provided.'  # the default reason for a ban
MIN_REJECT_AUTOBAN_ELIGIBLE = 3  # the minimum number of rejections (in a week) to be eligible for a ban
AUTOBAN_REJECT_ACCEPT_RATIO = 0.33  # the ratio of rejections / acceptances to be eligible for a ban
MAX_ATTEMPTS_PER_WEEK = np.inf # the maximum number of tasks a worker can complete per week



# TASK STATUSES
DOES_NOT_EXIST = '-2'  # the task does not exist
UNKNOWN_STATUS = '-1'  # the task exists, but has an unknown status
AWAITING_SERVE = '0'  # the task has not been begun
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

# SAMPLES REQ PER IMAGE
#   This is a lambda function that accepts the number of active images and computes the number of samples required for
#   effective ranking. This is adapted from the paper by Shah et al on Rank Centrality, which indicates that the number
#   of samples should be O(epsilon^-2 * n * poly(log(n))) where epsilon is the spectral gap of the Laplacian of the
#   graph. If pairs are chosen at random, hte graph is Erdos-Renyi and the spectral gap has a lower bound by some
#   probability and hence the complexity is O(n * poly(log(n))). poly(x) denotes x^O(1), which we assume to be x^1.
SAMPLES_REQ_PER_IMAGE = lambda n_active: n_active * np.log(n_active)