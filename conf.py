"""
Global configuration parameters for the mturk task. Note that at this point it isn't so much a configuration file as a
configuration + global parameters file.
"""

import numpy as np
import string
import random
import os
from utils import *


"""
For debugging
"""
TESTING = False
MTURK_SANDBOX = False


"""
AWS STUFF
"""
# MTURK -- image.inference@gmail.com
MTURK_ACCESS_ID = os.environ['MTURK_ACCESS_ID']
MTURK_SECRET_KEY = os.environ['MTURK_SECRET_KEY']
# AWS
AWS_ACCESS_ID = os.environ['AWS_ACCESS_ID']
AWS_SECRET_KEY = os.environ['AWS_SECRET_KEY']


"""
MTURK QUALIFICATIONS
"""
# CONSIDER MAKING THESE INTEGERS AND THEN JUST CHECKING THAT THE QUALIFICATION VALUE
# EQUALS WHAT WE EXPECT.
PASSED_PRACTICE = 200  # the user has passed the practice and can try doing 'real' tasks
# NEEDS_PRACTICE = 100  # the user has yet to pass a practice.
# TEMPORARY_BAN = 000  # the user has been temporarily banned.
# AVAILABLE_QUALIFICATIONS = (PASSED_PRACTICE, TEMPORARY_BAN)

# descriptions
QUALIFICATION_DESCRIPTION = dict()
QUALIFICATION_DESCRIPTION[PASSED_PRACTICE] = 'You have passed the practice and are ready to begin doing tasks.'
# QUALIFICATION_DESCRIPTION[TEMPORARY_BAN] = 'You have been temporarily banned. For more information, email us at ' \
#                                            'image.inference@gmail.com'

# notifications
QUALIFICATION_NOTIFICATION = dict()
QUALIFICATION_NOTIFICATION[PASSED_PRACTICE] = True  # notify them if they've passed the practice
# QUALIFICATION_NOTIFICATION[TEMPORARY_BAN] = False  # DON'T notify them if they've been temporarily banned

# names
PASSED_PRACTICE_NAME = 'Passed Practice'


"""
TASK CONFIGURATION
"""
# The practice instruction sequence
PRACTICE_INSTRUCTION_SEQUENCE = ['instructions/practice/practice-1.html',
                                 'instructions/practice/practice-2-keep-1.html',
                                 'instructions/practice/practice-3-keep-2.html',
                                 'instructions/practice/practice-4-reject-1.html',
                                 'instructions/practice/practice-5-reject-2.html',
                                 'instructions/practice/practice-6.html']
TASK_INSTRUCTION_SEQUENCE = []  # The default task instruction sequence for 'real' tasks
DEF_KEEP_BLOCK_INSTRUCTIONS = '/instructions/segment_preface/keep.html'  # def in-task keep segment instructions
DEF_REJECT_BLOCK_INSTRUCTIONS = '/instructions/segment_preface/reject.html'  # def in-task reject segment instructions
PRELOAD_IMAGES = True  # Whether or not to preload the images for the task.
BOX_SIZE = [800, 500]  # The box size for serving images, [w, h]
HIT_SIZE = [600, 400]  # the hit box size, which is where the images will be constrained to occur in, [w, h]
POS_TYPE = 'random'  # the position type, see make_html.py
ATTRIBUTE = 'interesting'  # the default attribute to use
IMAGE_ATTRIBUTES = []  # the types of images to be included in the task.
DEF_FEEDBACK_TIME = 100  # the amount of time to display feedback
DEF_TRIAL_TIME = 1500  # the maximum amount of time each trial is allowed to go for
DEF_INSTRUCTIONS = None  # the default instructions to display before each SEGMENT
DEF_RESPONSE_ENDS_TRIAL = 'true'  # whether or not a response ends a trial
DEF_TRIAL_TYPE = KEEP_BLOCK  # the default trial type
DEF_PROMPT = ''  # the default prompt to display
DEF_PRACTICE_PROMPT = '<font color="red">PRACTICE</font>'  # the default prompt to display during practice tasks


"""
OTHER MTURK INFORMATION
"""
PREVIEW_ASSIGN_ID = 'ASSIGNMENT_ID_NOT_AVAILABLE'  # what the assignment ID is when they're just previewing
EXTERNAL_QUESTION_ENDPOINT = ''  # where to route external question urls
MTURK_SANDBOX_HOST = 'mechanicalturk.sandbox.amazonaws.com'  # the host for the mturk sandbox
MTURK_HOST = ''  # the host for the vanilla sandbox
DEFAULT_TASK_PAYMENT = 0.40  # the default payment for
DEFAULT_PRACTICE_PAYMENT = 0.20  # the default payment for practices
DEFAULT_TASK_NAME = 'Choosing images'  # The title for the actual tasks.
DEFAULT_PRACTICE_TASK_NAME = 'Practice choosing images'  # The title for practice tasks.
HIT_LIFETIME_IN_SECONDS = 60*60*24*7  # How long a hit lasts. HITs remain for one week.
AUTO_APPROVE_DELAY = 60*60*24*3  # How long until the task is auto-accepted.
KEYWORDS = []
DESCRIPTION = 'Choosing %s images (no limit)' % ATTRIBUTE  # The default ask description.
PRACTICE_DESCRIPTION = 'Practice for choosing %s images (you only need pass the practice once)' % ATTRIBUTE # the
                                                                                        # default practice description
ASSIGNMENT_DURATION = 60*60*2  # How long after accepting a task does a worker have to complete it.
QUALIFICATION_ID = None  # this can't be known until we create the qualification type ID.


"""
WORKER OPTIONS
"""
MAX_PRACTICES_PER_WEEK = 5  # The number of times you can attempt a practice per week.
MAX_ATTEMPTS_PER_WEEK = np.inf  # the maximum number of tasks a worker can complete per week
DEFAULT_BAN_REASON = 'Reason not provided.'  # the default reason for a ban
MIN_REJECT_AUTOBAN_ELIGIBLE = 3  # the minimum number of rejections (in a week) to be eligible for a ban
AUTOBAN_REJECT_ACCEPT_RATIO = 0.33  # the ratio of rejections / acceptances to be eligible for a ban


"""
BLOCK IDENTIFICATION
"""
KEEP_BLOCK = 'keep'
REJECT_BLOCK = 'reject'


"""
TASK GENERATION
"""
MARGIN_SIZE = 2  # the default margin size, for formatting.
DEF_KEEP_BLOCKS = 1  # the default number of keep blocks for a task
DEF_REJECT_BLOCKS = 1  # the default number of reject blocks for a task
DEF_PRACTICE_KEEP_BLOCKS = 1  # the default number of keep blocks for a practice task
DEF_PRACTICE_REJECT_BLOCKS = 1  # the default number of reject blocks for a task
RANDOMIZE_SEGMENT_ORDER = False  # whether or not segments are randomized within task.
DEF_IMAGES_PER_TASK = None  # TODO: decide on this
DEF_IMAGES_PER_PRACTICE = None  # TODO: decide on this too


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


"""
DIRECTORIES
"""
ROOT = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(ROOT, 'generate/templates/')  # The template location
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
TABLE NAMES
"""
# See the readme on the database schema.
WORKER_TABLE = 'workers'
TASK_TABLE = 'tasks'
IMAGE_TABLE = 'images'
PAIR_TABLE = 'pairs'
WIN_TABLE = 'wins'
HIT_TYPE_TABLE = 'hittypes'
if TESTING:
    WORKER_TABLE = 'TEST_workers'
    TASK_TABLE = 'TEST_tasks'
    IMAGE_TABLE = 'TEST_images'
    PAIR_TABLE = 'TEST_pairs'
    WIN_TABLE = 'TEST_wins'
    HIT_TYPE = 'TEST_hittypes'


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
DATA INPUT FORMATTING
"""
TRUE = '1'  # how True is represented in the database
FALSE = '0'  # how False is represented in the database
FLOAT_STR = '%.4g'  # how to format strings / integers


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
ACTIVE_FILTER = ("SingleColumnValueFilter ('metadata','isActive',=,'regexstring:^%s$')" % TRUE)
# Finds tasks that are awaiting serving.
AWAITING_SERVE_FILTER = ("SingleColumnValueFilter ('status', 'awaitingServe', =, 'regexstring:^%s$')" % TRUE)
# Finds practice tasks.
IS_PRACTICE_FILTER = ("SingleColumnValueFilter ('metadata', 'isPractice', =, 'regexstring:^%s$')" % TRUE)
# Finds inactive images.
INACTIVE_FILTER = ("SingleColumnValueFilter ('metadata','isActive',=,'regexstring:^%s$')" % FALSE)
# Finds tasks that are pending completion
PENDING_COMPLETION_FILTER = ("SingleColumnValueFilter ('status', 'pendingCompletion', =, 'regexstring:^%s$')" % TRUE)