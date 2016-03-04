"""
These are the globals that we will use to populate templates in Jinja,
since so many of them we re-used and it became a pain to have to check
everything!

This exports a dictionary called 'jg', for Jinja2 Globals, which contain all the
redundant variables.
"""

from conf import *

resources = [
    'https://s3.amazonaws.com/mturk-webserver-assets/error.png',
    'https://s3.amazonaws.com/mturk-webserver-assets/check.png',
    'https://s3.amazonaws.com/mturk-webserver-assets/blocked.png',
    'https://s3.amazonaws.com/mturk-webserver-assets/stop.png',
    'https://s3.amazonaws.com/mturk-webserver-assets/attention.png'
]

scripts = [  # note, this also includes the jsPsych-specific CSS.
    'js/jspsych-4.3/js/jquery.min.js',
    'js/jspsych-4.3/js/jquery-ui.min.js',
    'js/jspsych-4.3/jspsych.js',
    'js/jspsych-4.3/plugins/jspsych-click-choice.js',
    'js/jspsych-4.3/plugins/jspsych-instructions.js',
    'js/jspsych-4.3/plugins/jspsych-html.js',
    'js/practice_debrief.js',
    'js/progressbar.min.js',
    'js/jspsych-4.3/css/jspsych.css',
    'js/jspsych-4.3/css/jquery-ui.css'
]


def _get_static_urls():
    """
    Accepts a request for a task, and then returns the static URLs pointing to
    all the resources.

    NOTES
        The template variables corresponding to the resources are generally
        named with their filename (no directory or folder information) +
        their extension.

        Getting this to work with Flask is somewhat opaque. Even though Flask
        is the most lightweight web framework that I can find, it seems
        ridiculously overpowered for what I'm doing. Thus, _get_static_url's
        will just return the hard-coded stuff for now.

    :return: A dictionary of static urls, of the form
            {'resource_name': 'resource_url'}
    """
    static_urls = dict()

    for resource in resources:
        static_urls[
            os.path.basename(resource).replace('.', '_').replace('-', '_')] = \
            resource
    for script in scripts:
        static_urls[
            os.path.basename(script).replace('.', '_').replace('-', '_')] = \
            os.path.join('static', script)
    static_urls['demographics'] = 'static/html/demographics.html'
    static_urls['success'] = 'static/html/success.html'
    static_urls['submit'] = EXTERNAL_QUESTION_SUBMISSION_ENDPOINT
    static_urls['attribute'] = ATTRIBUTE
    return static_urls

jg = _get_static_urls()

jg['attribute'] = ATTRIBUTE
jg['attribute_description'] = ATTRIBUTE_DESCRIPTION
jg['practice_task_name'] = DEFAULT_PRACTICE_TASK_NAME
jg['task_name'] = DEFAULT_TASK_NAME
jg['MAX_FRAC_TOO_FAST'] = MAX_FRAC_TOO_FAST
jg['MIN_TRIAL_RT'] = MIN_TRIAL_RT
jg['MAX_FRAC_UNANSWERED'] = MAX_FRAC_UNANSWERED
jg['MAX_PROB_RANDOM'] = MAX_PROB_RANDOM
jg['MAX_FRAC_CONTRADICTIONS'] = MAX_FRAC_CONTRADICTIONS

