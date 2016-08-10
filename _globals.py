"""
This file contains globals that are not meant to be readily edited. It was
necessitated for two reasons:
    - conf import utils, but utils required parameters in conf
    - conf became too large.
There are no hard-and-fast rules about what belongs in _globals vs. what belongs
in _conf.
"""

import os
import numpy as np

"""
MTURK QUALIFICATIONS
"""
# These names are misleading. QUALIFICATION_NAME is a relic from when there was
# only one qualification; in reality, both QUALIFICATION_NAME and
# DAILY_QUOTA_QUALIFICATION each represent distinct qualifications.

# this is the 'main' qualification -- which allows them to take the task.
QUALIFICATION_NAME = 'Practice for image selection task passed'
QUALIFICATION_DESCRIPTION = 'You have passed the practices for the image ' \
                            'selection task, and are now free to ' \
                            'complete the full tasks. Thank you! NOTE: This ' \
                            'qualification should not be requested! ' \
                            'It will be granted automatically after a short ' \
                            'delay following you completing the ' \
                            'practice task. NOTE: While our tasks have no ' \
                            'wrong answers, if we believe that you ' \
                            'are not giving your attention to the task or ' \
                            'behaving randomly, you could be ' \
                            'temporarily banned in which case this ' \
                            'qualification will be revoked. Once the ban ' \
                            'expires, you will be able to re-take the practice.'

# This is the quota counter qualification. This limits the number of
# tasks they can complete per day. It is  internally set, every day, to the
# value of MAX_SUBMITS_PER_DAY.
DAILY_QUOTA_NAME = 'Daily task limit'
DAILY_QUOTA_DESCRIPTION = 'This is the number of tasks you can submit per ' \
                          'day.  Every time you submit a task, it ' \
                          'decreases by one. Each day it is reset, so you ' \
                          'can resume doing tasks tomorrow. NOTE: You do ' \
                          'not need to apply for this. This is qualification' \
                          ' is granted automatically when you have received' \
                          ' the qualification "%s"' % QUALIFICATION_NAME

# This is the practice quota counter qualification. This limits the number of
# practices people can take per week.
PRACTICE_QUOTA_NAME = 'Weekly practice limit'
PRACTICE_QUOTA_DESCRIPTION = 'This is the number of practices you can ' \
                             'attempt per week. Because practices are never' \
                             ' rejected, we have to limit how many times ' \
                             'people can attempt to pass them.'

# This is the ban qualification, it's how we ban people.
BAN_QUALIFICATION_NAME = 'Temporary ban'
BAN_QUALIFICATION_DESCRIPTION = 'You have been temporarily banned for poor ' \
                                'performance. If you have any questions, ' \
                                'do not hesitate to contact us.'

"""
BLOCK IDENTIFICATION
"""
KEEP_BLOCK = 'keep'
REJECT_BLOCK = 'reject'


"""
WEBSERVER INFORMATION
"""
WEBSERVER_PORT = 8080
WEBSERVER_URL = "mturk.kryto.me"


"""
TASK GENERATION OPTIONS
"""
# ALLOW_MULTIPAIRS determines if task generation will reject candidate triplets
# that contain pairs we've already seen before.
ALLOW_MULTIPAIRS = True

"""
OTHER MTURK INFORMATION
"""
# what the assignment ID is when they're just previewing
PREVIEW_ASSIGN_ID = 'ASSIGNMENT_ID_NOT_AVAILABLE'
# where to route external question urls
EXTERNAL_QUESTION_ENDPOINT = 'https://%s/task' % WEBSERVER_URL
EXTERNAL_QUESTION_SUBMISSION_ENDPOINT = 'https://%s/submit' % WEBSERVER_URL

# the host for the mturk sandbox
MTURK_SANDBOX_HOST = 'mechanicalturk.sandbox.amazonaws.com'
# the host for the vanilla sandbox
MTURK_REGULAR_HOST = 'mechanicalturk.amazonaws.com'
KRYPTON_WID = 'A1RPGGKICRYDKC'  # Krypton's worker ID
KRYPTON_RID = 'A1RPGGKICRYDKC'  # Krypton's requester ID


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
ERROR_TEMPLATE = 'error.html'
DEMOGRAPHICS_TEMPLATE = 'demographics.html'
SUCCESS_TEMPLATE = 'success_pages/success.html'
SUCCESS_PRACTICE_PASSED_TEMPLATE = 'success_pages/practice_passed.html'
SUCCESS_PRACTICE_FAILED_TEMPLATE = 'success_pages/practice_failed.html'
SUCCESS_PRACTICE_ALREADY_PASSED_TEMPLATE = \
    'success_pages/practice_already_passed.html'
# preview templates
PREVIEW_TEMPLATE = 'preview/preview_template.html'
PREVIEW_TEMPLATE_NEED_PRACTICE = 'preview/preview_template_unqualified.html'
PREVIEW_TOO_MANY_TASKS = 'preview/preview_too_many_tasks.html'
PRACTICE_PREVIEW_TEMPLATE = 'preview/practice_preview_template.html'
PRACTICE_PREVIEW_QUOTA_EXCEEDED = 'preview/practice_preview_too_many_practice.html'
PRACTICE_PREVIEW_ALREADY_PASSED = 'preview/practice_preview_already_passed.html'

"""
HIT PAGE
"""
# of the form:
# https://workersandbox.mturk.com/mturk/searchbar?requesterId=A16DTSBAGJT9RT
# i.e.,
# https://workersandbox.mturk.com/mturk/searchbar?requesterId=<RID>

"""
DIRECTORIES
"""
ROOT = os.path.dirname(os.path.abspath(__file__))
LOG_LOCATION = os.path.join(os.path.expanduser('~'), 'mturk_logs',
                            'webserver.log')
# The template location
TEMPLATE_DIR = os.path.join(ROOT, 'static/resources/templates/')
# The destination of experiments
EXPERIMENT_DIR = os.path.join(ROOT, 'experiments/')


"""
TASK ID OPTIONS
"""
# the prefix for every true task ID
TASK_PREFIX = 't_'
# the prefix for every practice task ID
PRACTICE_PREFIX = 'p_'


"""
DATABASE OPTIONS
"""
# DATABASE_LOCATION = "10.0.36.202"
DATABASE_LOCATION = "10.0.43.96"  # new database
TEST_DATABASE_LOCATION = "10.0.53.47"
# if True, will store all practice pairs as if they were in real trials.
STORE_PRACTICE_PAIRS = False
# The number of images to activate in a chunk.
ACTIVATION_CHUNK_SIZE = 4000
INIT_BATCH_SIZE = 4000
# the default length of time for a ban
DEFAULT_BAN_LENGTH = float(60*60*24*7)
# the amount of time a task may be pending without being completed
TASK_COMPLETION_TIMEOUT = float(60*60*24)


"""
DATABASE FLAGS
"""
# used in filter generators, to indicate whether or not it must pass ANY filters
ANY = 'OR'
# used in filter generators, to indicate whether or not it must pass ALL filters
ALL = 'AND'


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
HIT_PENDING = 0  # unexpired, unsubmitted (no assignments approved)
HIT_EXPIRED = 1  # expired, unsubmitted (no assignments approved)
HIT_COMPLETE = 2  # submitted (all assignments submitted)
HIT_APPROVED = 3  # submitted, approved (all assignments submitted, approved)
HIT_REJECTED = 4  # submitted, rejected (all assignments submitted, rejected)
HIT_DISPOSED = 5  # disposed
HIT_DEAD= 6  # the hit isn't complete or expired but is unavailable


"""
PRACTICE STATUSES

Notes: DEAD supersedes COMPLETE
"""

PRACTICE_UNDEFINED = -1  # undefined status
PRACTICE_PENDING = 0  # practice is pending (not all complete)
PRACTICE_COMPLETE = 1  # practice is complete (all jobs finished)
PRACTICE_EXPIRED = 2  # th epractice is expired
PRACTICE_DEAD = 3  # practice is unavailable but not expired / complete


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
STATISTICS_TABLE = 'imagestats'
TASK_JSON_TABLE = 'taskjson'

"""
COLUMN NAMES, BY FAMILY
"""
# See the readme on the database schema.
WORKER_FAMILIES = {'status': dict(max_versions=1),
                   'stats': dict(max_versions=1),
                   'demographics': dict(max_versions=1),
                   'location': dict(max_versions=1)}
TASK_FAMILIES = {'metadata': dict(max_versions=1),
                 'status': dict(max_versions=1),
                 'completion_data': dict(max_versions=1),
                 'user_agent': dict(max_versions=1),
                 'validation_statistics': dict(max_versions=1),
                 'blocks': dict(max_versions=1),
                 'html': dict(max_versions=1)}
TASK_JSON_FAMILIES = {'data': dict(max_versions=1)}
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
STATISTICS_FAMILIES = {'statistics': dict(max_versions=1)}


"""
SAMPLES REQ PER IMAGE

Notes:

This is a lambda function that accepts the number of active images and
computes the number of samples required for effective ranking. This is
adapted from the paper by Shah et al on Rank Centrality, which indicates that
the number of samples should be O(epsilon^-2 * n * poly(log(n))) where epsilon
is the  spectral gap of the Laplacian of the graph. If pairs are chosen at
random, the graph is Erdos-Renyi and the spectral gap has a lower bound by
some probability and hence the complexity is O(n * poly(log(n))). poly(x)
denotes x^O(1), which we assume to be x^1.
"""
_gamma = 2.  # the gamma multiplier, O(gamma n log(n))
MEAN_SAMPLES_REQ_PER_IMAGE = lambda n_active: _gamma * np.log(n_active)


"""
VARIOUS IMPORTANT FILTERS
"""
# Finds active images.
ACTIVE_FILTER = ("SingleColumnValueFilter ('metadata','is_active',=,"
                 "'regexstring:^%s$')" % TRUE)
# Finds tasks that are awaiting serving.
AWAITING_SERVE_FILTER = ("SingleColumnValueFilter ('status', 'awaiting_serve', "
                         "=, 'regexstring:^%s$')" % TRUE)
# Finds practice tasks.
IS_PRACTICE_FILTER = ("SingleColumnValueFilter ('metadata', 'is_practice', =, "
                      "'regexstring:^%s$')" % TRUE)
# Finds inactive images.
INACTIVE_FILTER = ("SingleColumnValueFilter ('metadata','is_active',=,"
                   "'regexstring:^%s$')" % FALSE)
# Finds tasks that are pending completion
PENDING_COMPLETION_FILTER = ("SingleColumnValueFilter ('status', "
                             "'pending_completion', =, "
                             "'regexstring:^%s$')" % TRUE)


"""
MONITORING CONFIGURATION
"""
# the monitoring server to use (we're piggybacking on Neon's)
MONITORING_CARBON_SERVER = "52.70.225.116"
MONITORING_CARBON_PORT = 8090
MONITORING_SERVICE_NAME = "mturk"  # not clear what this is.
MONITORING_SLEEP_INTERVAL = 60
