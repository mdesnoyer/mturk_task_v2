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
DEF_PRACTICE_NUM_IMAGES_PER_TASK = 9  # the default number of images per practice.
# The practice instruction sequence
DEF_INTRO_INSTRUCTIONS = '/instructions/normal_template.html'  # the default introduction instructions
DEF_PRACTICE_INTO_INSTRUCTIONS = '/instructions/practice_template.html'  # the default practice introduction
DEF_KEEP_BLOCK_INSTRUCTIONS = ['/instructions/segment_preface/keep.html']  # def in-task keep segment instructions
DEF_REJECT_BLOCK_INSTRUCTIONS = ['/instructions/segment_preface/reject.html']  # def in-task reject segment instructions
BOX_SIZE = [800, 500]  # The box size for serving images, [w, h]
HIT_SIZE = [600, 400]  # the hit box size, which is where the images will be constrained to occur in, [w, h]
POS_TYPE = 'random'  # the position type, see make_html.py
ATTRIBUTE = 'interesting'  # the default attribute to use
IMAGE_ATTRIBUTES = []  # the types of images to be included in the task.
TIMING_POST_TRIAL = 200  # Sets the time, in milliseconds, between the current trial and the next trial.
DEF_FEEDBACK_TIME = 100  # the amount of time to display feedback
DEF_TRIAL_TIME = 3000  # the maximum amount of time each trial is allowed to go for
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
TASK_VALIDATION
"""
# TODO: Change debrief js to use these values.
MAX_MEAN_RT = np.inf
MIN_MEAN_RT = 200
MAX_PROB_RANDOM = 0.8
MAX_FRAC_CONTRADICTIONS = 0.3
MAX_FRAC_UNANSWERED = 0.3

"""
TASK VALIDATION FEEDBACK
"""
BAD_DATA_TOO_SLOW = 'Your responses are too slow, please try to work fast.'
BAD_DATA_TOO_FAST = 'Your responses are too fast! Be sure you are not clicking randomly.'
BAD_DATA_CLICKING = 'You are clicking in the same place too often. Pleased click based on the images themselves, ' \
                    'not their position.'
BAD_DATA_TOO_CONTRADICTORY = 'You are making too many contradictions. Please do not click around randomly, but make ' \
                             'deliberate choices.'
BAD_DATA_TOO_MANY_UNANSWERED = 'You are not answering too many of the trials.'


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