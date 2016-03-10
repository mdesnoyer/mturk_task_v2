"""
Global configuration parameters for the mturk task.

Once upon a time, this used to be a single file, but everything necessary is
now spread out among:
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
LOCAL = False  # True if you're running on a local machine, False if AWS
_USE_OPSWORKS_DB = True  # True if you're going to be using the opsworks
# database (i.e., not the local one)...for which you will need to have an SSH
# tunnel opened! (see intro notes in webserver.py)

"""
AWS STUFF
"""
# MTURK -- image.inference@gmail.com
MTURK_ACCESS_ID = 'AKIAJPB4VQCDGOUQK2JA'
MTURK_SECRET_KEY = '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'
# AWS
AWS_ACCESS_ID = None  # os.environ['AWS_ACCESS_ID']
AWS_SECRET_KEY = None  # os.environ['AWS_SECRET_KEY']
IMAGE_BUCKET = 'neon-image-library'  # the location on S3 of the images

"""
GLOBAL TASK APPEARANCE CONFIGURATION
"""
BOX_SIZE = [800, 500]  # The box size for serving images, [w, h]
HIT_SIZE = [600, 400]  # the hit box size, which is where the images will be
                       # constrained to occur in, [w, h]
MARGIN_SIZE = 2  # the default margin size, for formatting.


"""
GLOBAL WITHIN-TASK TIMING CONFIGURATION
"""
TIMING_POST_TRIAL = 200  # Sets the time, in milliseconds, between the current
                         # trial and the next trial.
DEF_FEEDBACK_TIME = 100  # the amount of time to display feedback
DEF_TRIAL_TIME = 3000  # the maximum amount of time each trial is allowed to
                       # go for


"""
GLOBAL ACROSS-TASK TIMING CONFIGURATION
"""
HIT_LIFETIME_IN_SECONDS = 60*60*24*30  # How long a hit lasts. The current value
                                       # is one month.
AUTO_APPROVE_DELAY = 0      # How long until the task is auto-accepted, in
                            # seconds. Currently assignments are auto-approved.
HIT_TYPE_DURATION = 60*60*24*365  # How long registered hit types last, in
                                  # seconds. The current value is one year.
ASSIGNMENT_DURATION = 60*60*2  # How long after accepting a task does a worker
                               # have to complete it.
DEF_EXTENSION_TIME = 60*60*3  # The default time to extend a hit for during
                              # extension. The current value is 3 days.


"""
TASK CONFIGURATION
"""
DEF_NUM_IMAGES_PER_TASK = 198   # the default number of images that appear in a
                               # block.
DEF_NUM_IMAGE_APPEARANCE_PER_TASK = 1  # the default number of image
                                       # appearances per task. [no practice
                                       # analogue]
DEF_INTRO_INSTRUCTIONS = \
    '/instructions/normal_template.html'  # the default introduction
                                          # instructions
DEF_KEEP_BLOCK_INSTRUCTIONS = \
    ['/instructions/segment_preface/keep.html']  # def in-task keep segment
                                                 # instructions
DEF_REJECT_BLOCK_INSTRUCTIONS = \
    ['/instructions/segment_preface/reject.html']  # def in-task reject segment
                                                   # instructions
POS_TYPE = 'random'  # the position type, see make_html.py
ATTRIBUTE = 'eye catching'  # the default attribute to use
# the attribute description is an elaboration of what we mean by ATTRIBUTE
ATTRIBUTE_DESCRIPTION = 'What we mean by EYE CATCHING is images that you ' \
                        'want to' \
                        ' know more about. For instance, if that image were ' \
                        'the thumbnail for a video, would you want to watch ' \
                        'that video? Or if that image were from a gallery, ' \
                        'would you want to see the rest of the gallery? In ' \
                        'essence, \'EYE CATCHING\' means images that you ' \
                        'think' \
                        ' are beautiful, compelling, intriguing, or which ' \
                        'grab your attention.'
IMAGE_ATTRIBUTES = []  # the types of images to be included in the task.
DEF_INSTRUCTIONS = ''  # the default instructions to display before each SEGMENT
DEF_TRIAL_TYPE = KEEP_BLOCK  # the default trial type
DEF_PROMPT = ''  # the default prompt to display
DEF_KEEP_BLOCKS = 1  # the default number of keep blocks for a task
DEF_REJECT_BLOCKS = 1  # the default number of reject blocks for a task
RANDOMIZE_SEGMENT_ORDER = False  # whether or not segments are randomized
                                 # within task.
#DEFAULT_TASK_PAYMENT = 0.45  # the default payment for tasks
# we are computing the default task payment as:
# (number of tuples to rate per segment) * (number of segments) * (secs per
# tuple) * (price per minute)
_DEFAULT_TASK_PAYMENT = (DEF_NUM_IMAGES_PER_TASK / 3. *
                         DEF_NUM_IMAGE_APPEARANCE_PER_TASK) * 2 * \
                        DEF_TRIAL_TIME / 1000 * (0.125 / 60)
DEFAULT_TASK_PAYMENT = float(int(_DEFAULT_TASK_PAYMENT * 100))/100
DEFAULT_TASK_NAME = 'Choosing images'  # The title for the actual tasks.
DESCRIPTION = ('Choosing %s images. These HITs are always instantly accepted.' %
               ATTRIBUTE)
# The
# default ask description.
KEYWORDS = []  # the keywords for the HIT / HIT Type

"""
PRACTICE TASK CONFIGURATION
"""
DEF_PRACTICE_NUM_IMAGES_PER_TASK = 30  # the default number of images
                                       # per practice.
DEF_PRACTICE_INTO_INSTRUCTIONS = \
    '/instructions/practice_template.html'  # the default practice introduction
DEF_RESPONSE_ENDS_TRIAL = 'true'  # whether or not a response ends a trial
DEF_PRACTICE_PROMPT = \
    '<font color="red">PRACTICE</font>'  # the default prompt  to display
                                         # during practice tasks
DEF_PRACTICE_KEEP_BLOCKS = 1  # the default number of keep blocks for a
                              # practice task
DEF_PRACTICE_REJECT_BLOCKS = 1  # the default number of reject blocks for a task
DEFAULT_PRACTICE_PAYMENT = 0.10  # the default payment for practices
DEFAULT_PRACTICE_TASK_NAME = \
    'Practice choosing images'  # The title for practice tasks.
PRACTICE_DESCRIPTION = \
    ('Practice for choosing %s images (you only need pass the practice once). '
     'The required qualifications are automatically granted, be sure to '
     'request them before you begin. These HITs are always instantly '
     'accepted.' % ATTRIBUTE)


"""
TASK_VALIDATION
"""
# TODO: Change debrief js to use these values.
MAX_MEAN_RT = np.inf
MIN_TRIAL_RT = 400
MAX_PROB_RANDOM = 0.95
MAX_FRAC_CONTRADICTIONS = 0.3
MAX_FRAC_UNANSWERED = 0.2
MAX_FRAC_TOO_FAST = 0.2


"""
TASK VALIDATION FEEDBACK
"""
BAD_DATA_TOO_SLOW = \
    'Your responses are too slow, please try to work fast.'
BAD_DATA_TOO_FAST = \
    'Your responses are too fast! Be sure you are not clicking randomly.'
BAD_DATA_CLICKING = \
    'You are clicking in the same place too often. Pleased click based on ' \
    'the images themselves, not their position.'
BAD_DATA_TOO_CONTRADICTORY = \
    'You are making too many contradictions. Please do not click around ' \
    'randomly, but make deliberate choices.'
BAD_DATA_TOO_MANY_UNANSWERED = 'You are not answering too many of the trials.'


"""
WORKER OPTIONS
"""
# the maximum number of tasks a worker can complete per day
MAX_SUBMITS_PER_DAY = 100
# the default reason for a ban
DEFAULT_BAN_REASON = 'You have been banned for one week from completing our ' \
                     'tasks due to poor task performance.'
# The number of submitted tasks that will result in the worker having their
# counts reset.
TASK_SUBMISSION_RESET_VALUE = MAX_SUBMITS_PER_DAY * 7
# the minimum number of rejections (in a week) to be eligible for a ban
MIN_REJECT_AUTOBAN_ELIGIBLE = 8
# the ratio of rejections / acceptances to be eligible for a ban
AUTOBAN_REJECT_ACCEPT_RATIO = 0.33
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


"""
WEBSERVER CONFIGURATION
"""
NUM_THREADS = 1  # the number of threads to use on the webserver.
ENABLE_BANNING = True  # whether or not to ban poor-performing workers.
CONTINUOUS_MODE = True  # whether or not to run the task continuously.


"""
MTURK OPTIONS
"""
NUM_PRACTICES = 5  # how many practices to post at once
NUM_TASKS = 5  # how many tasks to maintain online
NUM_ASSIGNMENTS_PER_PRACTICE = 1      # how many people can take a given
                                      # practice?
PRACTICE_TASK_LIFETIME = 60*60*24*7   # the time a practice task exists for.
                                      # default is 1 week.

if LOCAL:
    # change a bunch of the '_global' parameters
    WEBSERVER_PORT = 12344
    WEBSERVER_URL = "127.0.0.1"  # mturk.neon-lab.com
    EXTERNAL_QUESTION_ENDPOINT = \
        'https://%s:%i/task' % (WEBSERVER_URL, WEBSERVER_PORT)
    EXTERNAL_QUESTION_SUBMISSION_ENDPOINT = \
        'https://%s:%i/submit' % (WEBSERVER_URL,  WEBSERVER_PORT)
    if not _USE_OPSWORKS_DB:
        DATABASE_LOCATION = "localhost"