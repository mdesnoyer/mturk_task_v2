"""
This file contains globals that are not meant to be readily edited. It was necessitated for two reasons:
    - conf import utils, but utils required parameters in conf
    - conf became too large.
There are no hard-and-fast rules about what belongs in _globals vs. what belongs in _conf.
"""

import os
import numpy as np

"""
MTURK QUALIFICATIONS
"""
# These names are misleading. QUALIFICATION_NAME is a relic from when there was only one qualification; in reality, both
# QUALIFICATION_NAME and DAILY_QUOTA_QUALIFICATION each represent distinct qualifications.

# this is the 'main' qualification -- which allows them to take the task.
QUALIFICATION_NAME = 'Practice for image selection task passed.'
QUALIFICATION_DESCRIPTION = 'You have passed the practices for the image selection task, and are now free to ' \
                            'complete the full tasks. Thank you! NOTE: This qualification should not be requested! ' \
                            'It will be granted automatically after a short delay following you completing the ' \
                            'practice task. NOTE: While our tasks have no wrong answers, if we believe that you ' \
                            'are not giving your attention to the task or behaving randomly, you could be ' \
                            'temporarily banned in which case this qualification will be revoked. Once the ban ' \
                            'expires, you will be able to re-take the practice.'

# This is the qualification counter qualification. This limits the number of tasks they can complete per day. It is
# internally set, every day, to the value of MAX_SUBMITS_PER_DAY.
DAILY_QUOTA_NAME = 'Daily task limit'
DAILY_QUOTA_DESCRIPTION = 'This is the number of tasks you can submit per day. Every time you submit a task, it ' \
                          'decreases by one. Each day it is reset, so you can resume doing tasks tomorrow. NOTE: ' \
                          'You do not need to apply for this. This is qualification is granted automatically ' \
                          'when you have received the qualification "%s"' % QUALIFICATION_NAME


"""
BLOCK IDENTIFICATION
"""
KEEP_BLOCK = 'keep'
REJECT_BLOCK = 'reject'


"""
OTHER MTURK INFORMATION
"""
PREVIEW_ASSIGN_ID = 'ASSIGNMENT_ID_NOT_AVAILABLE'  # what the assignment ID is when they're just previewing
# TODO: FUCK I HOPE PYRAMID SUPPORTS HTTPS!!
EXTERNAL_QUESTION_ENDPOINT = 'https://127.0.0.1:12344/task'  # where to route external question urls
MTURK_SANDBOX_HOST = 'mechanicalturk.sandbox.amazonaws.com'  # the host for the mturk sandbox
MTURK_HOST = 'mechanicalturk.amazonaws.com'  # the host for the vanilla sandbox
HIT_CHUNK_SIZE = 1000  # the number of tasks to post in blocks.


"""
TEMPLATES
"""
BASE_TEMPLATE = 'base.html'
PRELOAD_TEMPATE = 'preload_template.html'
INSTRUCTION_TEMPLATE = 'inst_template.html'
TRIAL_BLOCK_TEMPLATE = 'trial_block_template.html'
BAN_TEMPLATE = 'ban_page.html'
PRACTICE_EXCEEDED_TEMPLATE = 'too_many_practices.html'
PRACTICE_IM_DIR = 'instr_screenshots/'
ERROR_TEMPLATE = 'fetch_error.html'
DEMOGRAPHICS_TEMPLATE = 'demographics.html'
SUCCESS_TEMPLATE = 'success.html'


"""
DIRECTORIES
"""
ROOT = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(ROOT, 'resources/templates/')  # The template location
EXPERIMENT_DIR = os.path.join(ROOT, 'experiments/')  # The destination of experiments


"""
TASK ID OPTIONS
"""
TASK_PREFIX = 't_'  # the prefix for every true task ID
PRACTICE_PREFIX = 'p_'  # the prefix for every practice task ID


"""
DATABASE OPTIONS
"""
DATABASE_LOCATION = 'localhost'
STORE_PRACTICE_PAIRS = False  # if True, will store all practice pairs as if they were in real trials.
ACTIVATION_CHUNK_SIZE = 500  # The number of images to activate in a chunk.
DEFAULT_BAN_LENGTH = float(60*60*24*7)  # the default length of time for a ban
TASK_COMPLETION_TIMEOUT = float(60*60*24)  # the amount of time a task may be pending without being completed


"""
DATABASE FLAGS
"""
ANY = 'OR'  # used in filter generators, to indicate whether or not it must pass ANY filters
ALL = 'AND'  # used in filter generators, to indicate whether or not it must pass ALL filters


"""
TASK STATUSES
"""
DOES_NOT_EXIST = '-2'  # the task does not exist
UNKNOWN_STATUS = '-1'  # the task exists, but has an unknown status
AWAITING_HIT = '0'  # the task is awaiting a HIT group assignment.
AWAITING_SERVE = '1'  # the task has not been begun
IS_PRACTICE = '2'  # the task is a practice
COMPLETION_PENDING = '3'  # waiting on the worker to complete the task
EVALUATION_PENDING = '4'  # waiting on the task to be evaluated
ACCEPTED = '5'  # the task has been accepted
REJECTED = '6'  # the task has been rejected


"""
HIT STATUSES
"""
HIT_UNDEFINED = -1  # the HIT status is undefined.
HIT_PENDING = 0  # the HIT is: unexpired, unsubmitted (no assignments approved)
HIT_EXPIRED = 1  # the HIT is: expired, unsubmitted (no assignments approved)
HIT_COMPLETE = 2  # the HIT is: submitted (all assignments submitted)
HIT_APPROVED = 3  # the HIT is: submitted, approved (all assignments submitted, approved)
HIT_DISPOSED = 4  # the HIT is: disposed


"""
DATA INPUT FORMATTING
"""
TRUE = '1'  # how True is represented in the database
FALSE = '0'  # how False is represented in the database
FLOAT_STR = '%.4g'  # how to format strings / integers


"""
TABLE NAMES
"""
# See the readme on the database schema.
WORKER_TABLE = 'workers'
TASK_TABLE = 'tasks'
IMAGE_TABLE = 'images'
PAIR_TABLE = 'pairs'
WIN_TABLE = 'wins'
HIT_TYPE_TABLE = 'hittypes'


"""
COLUMN NAMES, BY FAMILY
"""
# See the readme on the database schema.
WORKER_FAMILIES = {'status': dict(max_versions=1),
                   'stats': dict(max_versions=1),
                   'demographics': dict(max_versions=1)}
TASK_FAMILIES = {'metadata': dict(max_versions=1),
                 'status': dict(max_versions=1),
                 'completed_data': dict(max_versions=1),
                 'blocks': dict(max_versions=1),
                 'html': dict(max_versions=1)}
IMAGE_FAMILIES = {'metadata': dict(max_versions=1),
                  'stats': dict(max_versions=1),
                  'phash': dict(max_versions=1),
                  'colorname': dict(max_versions=1),
                  'attributes': dict(max_versions=1)}
PAIR_FAMILIES = {'metadata': dict(max_versions=1),
                 'legacy_trials': dict(max_versions=1),
                 'legacy_workers': dict(max_versions=1)}
WIN_FAMILIES = {'data': dict(max_versions=1)}
HIT_TYPE_FAMILIES = {'metadata': dict(max_versions=1),
                     'status': dict(max_versions=1)}


"""
SAMPLES REQ PER IMAGE
"""
#   This is a lambda function that accepts the number of active images and computes the number of samples required for
#   effective ranking. This is adapted from the paper by Shah et al on Rank Centrality, which indicates that the number
#   of samples should be O(epsilon^-2 * n * poly(log(n))) where epsilon is the spectral gap of the Laplacian of the
#   graph. If pairs are chosen at random, hte graph is Erdos-Renyi and the spectral gap has a lower bound by some
#   probability and hence the complexity is O(n * poly(log(n))). poly(x) denotes x^O(1), which we assume to be x^1.
SAMPLES_REQ_PER_IMAGE = lambda n_active: n_active * np.log(n_active)


"""
VARIOUS IMPORTANT FILTERS
"""
# Finds active images.
ACTIVE_FILTER = ("SingleColumnValueFilter ('metadata','is_active',=,'regexstring:^%s$')" % TRUE)
# Finds tasks that are awaiting serving.
AWAITING_SERVE_FILTER = ("SingleColumnValueFilter ('status', 'awaiting_serve', =, 'regexstring:^%s$')" % TRUE)
# Finds practice tasks.
IS_PRACTICE_FILTER = ("SingleColumnValueFilter ('metadata', 'is_practice', =, 'regexstring:^%s$')" % TRUE)
# Finds inactive images.
INACTIVE_FILTER = ("SingleColumnValueFilter ('metadata','is_active',=,'regexstring:^%s$')" % FALSE)
# Finds tasks that are pending completion
PENDING_COMPLETION_FILTER = ("SingleColumnValueFilter ('status', 'pending_completion', =, 'regexstring:^%s$')" % TRUE)