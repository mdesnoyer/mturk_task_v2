"""
Configuration for Task generation.
"""

# The number of times you can attempt a practice per week.
MAX_PRACTICES_PER_WEEK = 5

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
PRELOAD_IMAGES = True  # Whether or not to preload the images for the task.
BOX_SIZE = [800, 500]  # The box size for serving images, [w, h]
HIT_SIZE = [600, 400]  # the hit box size, which is where the images will be constrained to occur in, [w, h]
POS_TYPE = 'random'  # the position type, see make_html.py
ATTRIBUTE = 'interesting'  # the default attribute to use
DEF_FEEDBACK_TIME = 100  # the amount of time to display feedback
DEF_TRIAL_TIME = 1500  # the maximum amount of time each trial is allowed to go for
DEF_INSTRUCTIONS = None  # the default instructions to display before each SEGMENT
DEF_RESPONSE_ENDS_TRIAL = 'true'  # whether or not a response ends a trial
DEF_TRIAL_TYPE = 'keep'  # the default trial type
DEF_PROMPT = ''  # the default prompt to display
DEF_PRACTICE_PROMPT = '<font color="red">PRACTICE</font>'  # the default prompt to display during practice tasks
MARGIN_SIZE = 2  # the default margin size, for formatting.

# Base templates
BASE_TEMPLATE = 'base.html'
PRELOAD_TEMPATE = 'preload_template.html'
INSTRUCTION_TEMPLATE = 'inst_template.html'
TRIAL_BLOCK_TEMPLATE = 'trial_block_template.html'
PRACTICE_IM_DIR = 'instr_screenshots/'

ROOT = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(ROOT, 'templates/')  # The template location
EXPERIMENT_DIR = os.path.join(ROOT, 'experiments/')  # The destination of experiments