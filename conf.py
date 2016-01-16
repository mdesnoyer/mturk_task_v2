"""
Global configuration parameters for the mturk task. Note that at this point it isn't so much a configuration file as a
configuration + global parameters file.
"""

import numpy as np
import string
import random
import sys
import os


base_path = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if base_path not in sys.path:
    sys.path.insert(0,base_path)


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
DEF_FEEDBACK_TIME = 100  # the amount of time to display feedback
DEF_TRIAL_TIME = 1500  # the maximum amount of time each trial is allowed to go for
DEF_INSTRUCTIONS = None  # the default instructions to display before each SEGMENT
DEF_RESPONSE_ENDS_TRIAL = 'true'  # whether or not a response ends a trial
DEF_TRIAL_TYPE = KEEP_BLOCK  # the default trial type
DEF_PROMPT = ''  # the default prompt to display
DEF_PRACTICE_PROMPT = '<font color="red">PRACTICE</font>'  # the default prompt to display during practice tasks

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
PRACTICE_IM_DIR = 'instr_screenshots/'

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
STORE_PRACTICE_PAIRS = False  # if True, will store all practice pairs as if they were in real trials.
ACTIVATION_CHUNK_SIZE = 500  # The number of images to activate in a chunk.
DEFAULT_BAN_LENGTH = float(60*60*24*7)  # the default length of time for a ban
TASK_COMPLETION_TIMEOUT = float(60*60*24)  # the amount of time a task may be pending without being completed


"""
TASK STATUSES
"""
DOES_NOT_EXIST = '-2'  # the task does not exist
UNKNOWN_STATUS = '-1'  # the task exists, but has an unknown status
AWAITING_SERVE = '0'  # the task has not been begun
IS_PRACTICE = '1'  # the task is a practice
COMPLETION_PENDING = '2'  # waiting on the worker to complete the task
EVALUATION_PENDING = '3'  # waiting on the task to be evaluated
ACCEPTED = '4'  # the task has been accepted
REJECTED = '5'  # the task has been rejected


"""
TABLE NAMES
"""
# See the readme on the database schema.
WORKER_TABLE = 'workers'
TASK_TABLE = 'tasks'
IMAGE_TABLE = 'images'
PAIR_TABLE = 'pairs'
WIN_TABLE = 'wins'

"""
COLUMN NAMES, BY FAMILY
"""
# See the readme on the database schema.
WORKER_FAMILIES = {'status': dict(max_versions=1),
                   'stats': dict(max_versions=1),
                   'demographics': dict(max_versions=1),
                   'attempted_practices': dict(max_versions=1)}
TASK_FAMILIES = {'metadata': dict(max_versions=1),
                 'status': dict(max_versions=1),
                 'completed_data': dict(max_versions=1),
                 'blocks': dict(max_versions=1),
                 'html': dict(max_versions=1),
                 'forbidden_workers': dict(max_versions=1)}
IMAGE_FAMILIES = {'metadata': dict(max_versions=1),
                  'stats': dict(max_versions=1),
                  'phash': dict(max_versions=1),
                  'colorname': dict(max_versions=1)}
PAIR_FAMILIES = {'metadata': dict(max_versions=1),
                 'legacy_trials': dict(max_versions=1),
                 'legacy_workers': dict(max_versions=1)}
WIN_FAMILIES = {'data': dict(max_versions=1)}


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
ID GENERATION
"""
_id_len = 16
def _rand_id_gen(n):
    """
    Generates random IDs
    :param n: The number of characters in the random ID
    :return: A raw ID string, composed of n upper- and lowercase letters as well as digits.
    """
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(n))


def task_id_gen():
    """
    Generates task IDs
    :return: A task ID, as a string
    """
    return TASK_PREFIX + _rand_id_gen(_id_len)


def practice_id_gen():
    """
    Generates practice IDs
    :return: A practice ID, as a string
    """
    return PREFIX_PREFIX + _rand_id_gen(_id_len)


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