"""
Global configuration parameters for the mturk task.

Once upon a time, this used to be a single file, but everything necessary is now spread out among:
    conf.py     <--- configurable params
    _globals.py <--- global parameters, shouldn't need changing too often
    _utils.py   <--- utility functions that are widely used.
"""

from _globals import *  # i know this is redundant but WHATEVER.
from _utils import *

"""
For debugging
"""
TESTING = False
MTURK_SANDBOX = True


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
TASK CONFIGURATION
"""
# TODO: define a pay per triplet quantity?
# TODO: test that this is an appropriate number
DEF_NUM_IMAGES_PER_TASK = 100  # the default number of images that appear in a block.
# TODO: test that this is an appropriate number
DEF_NUM_IMAGE_APPEARANCE_PER_TASK = 3  # the default number of image appearances per task. [no practice analogue]
# TODO: test that this is an appropriate number
DEF_PRACTICE_NUM_IMAGES_PER_TASK = 21  # the default number of images per practice.
# The practice instruction sequence
PRACTICE_INSTRUCTION_SEQUENCE = ['instructions/practice/practice-1.html',
                                 'instructions/practice/practice-2-keep-1.html',
                                 'instructions/practice/practice-3-keep-2.html',
                                 'instructions/practice/practice-4-reject-1.html',
                                 'instructions/practice/practice-5-reject-2.html',
                                 'instructions/practice/practice-6.html']
TASK_INSTRUCTION_SEQUENCE = []  # The default task instruction sequence for 'real' tasks
DEF_KEEP_BLOCK_INSTRUCTIONS = ['/instructions/segment_preface/keep.html']  # def in-task keep segment instructions
DEF_REJECT_BLOCK_INSTRUCTIONS = ['/instructions/segment_preface/reject.html']  # def in-task reject segment instructions
PRELOAD_IMAGES = True  # Whether or not to preload the images for the task.
BOX_SIZE = [800, 500]  # The box size for serving images, [w, h]
HIT_SIZE = [600, 400]  # the hit box size, which is where the images will be constrained to occur in, [w, h]
POS_TYPE = 'random'  # the position type, see make_html.py
ATTRIBUTE = 'interesting'  # the default attribute to use
IMAGE_ATTRIBUTES = []  # the types of images to be included in the task.
DEF_FEEDBACK_TIME = 100  # the amount of time to display feedback
DEF_TRIAL_TIME = 1500  # the maximum amount of time each trial is allowed to go for
DEF_INSTRUCTIONS = ''  # the default instructions to display before each SEGMENT
DEF_RESPONSE_ENDS_TRIAL = 'true'  # whether or not a response ends a trial
DEF_TRIAL_TYPE = KEEP_BLOCK  # the default trial type
DEF_PROMPT = ''  # the default prompt to display
DEF_PRACTICE_PROMPT = '<font color="red">PRACTICE</font>'  # the default prompt to display during practice tasks
MARGIN_SIZE = 2  # the default margin size, for formatting.
DEF_KEEP_BLOCKS = 1  # the default number of keep blocks for a task
DEF_REJECT_BLOCKS = 1  # the default number of reject blocks for a task
DEF_PRACTICE_KEEP_BLOCKS = 1  # the default number of keep blocks for a practice task
DEF_PRACTICE_REJECT_BLOCKS = 1  # the default number of reject blocks for a task
RANDOMIZE_SEGMENT_ORDER = False  # whether or not segments are randomized within task.


"""
OTHER MTURK INFORMATION
"""
DEFAULT_TASK_PAYMENT = 0.40  # the default payment for
DEFAULT_PRACTICE_PAYMENT = 0.20  # the default payment for practices
DEFAULT_TASK_NAME = 'Choosing images'  # The title for the actual tasks.
DEFAULT_PRACTICE_TASK_NAME = 'Practice choosing images'  # The title for practice tasks.
HIT_LIFETIME_IN_SECONDS = 60*60*24*7  # How long a hit lasts. The current value is one week.
AUTO_APPROVE_DELAY = 60*60  # How long until the task is auto-accepted, in seconds. The current value is 1 hour.
HIT_TYPE_DURATION = 60*60*24*365  # How long registered hit types last, in seconds. The current value is one year.
KEYWORDS = []  # the keywords for the HIT / HIT Type
DESCRIPTION = 'Choosing %s images' % ATTRIBUTE  # The default ask description.
PRACTICE_DESCRIPTION = 'Practice for choosing %s images (you only need pass the practice once)' % ATTRIBUTE
ASSIGNMENT_DURATION = 60*60*2  # How long after accepting a task does a worker have to complete it.
DEF_EXTENSION_TIME = 60*60*3  # The default time to extend a hit for during extension. The current value is 3 days.


"""
WORKER OPTIONS
"""
MAX_PRACTICES_PER_WEEK = 5  # The number of times you can attempt a practice per week.
MAX_SUBMITS_PER_DAY = 20  # the maximum number of tasks a worker can complete per day
DEFAULT_BAN_REASON = 'Reason not provided.'  # the default reason for a ban
MIN_REJECT_AUTOBAN_ELIGIBLE = 3  # the minimum number of rejections (in a week) to be eligible for a ban
AUTOBAN_REJECT_ACCEPT_RATIO = 0.33  # the ratio of rejections / acceptances to be eligible for a ban


"""
ALLOWABLE COUNTRIES
"""
# these are countries from which we accept workers
LOCALES = [
    'GB',  # great britain, northern ireland
    'IE',  # ireland
    'NZ',  # new zealand
    'AU',  # australia
    'BM',  # bermuda
    'VI',  # U.S. Virgin Islands
    'US',  # United States
    'CA'  # Canada
]


# this is a necessary switch. even though it should be in _globals, TESTING is a parameter.
if TESTING:
    WORKER_TABLE = 'TEST_workers'
    TASK_TABLE = 'TEST_tasks'
    IMAGE_TABLE = 'TEST_images'
    PAIR_TABLE = 'TEST_pairs'
    WIN_TABLE = 'TEST_wins'
    HIT_TYPE = 'TEST_hittypes'