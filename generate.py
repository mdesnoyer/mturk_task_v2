"""
Exports functions specific to integrating information from MTurk and the
database to generate the task HTML itself.  The external resources in
./resources are specifically in the purview of generate.py.
"""

from conf import *
import urllib
import cStringIO
from PIL import Image
import numpy as np
import jinja2
from jinja2 import meta
from jinja_globals import jg

_log = logger.setup_logger(__name__)

# create the jinja escape filter
def jinja2_escapejs_filter(value):
    # Perform Jinja2 Escapes
    _js_escapes = {'\\': '\\u005C', '\'': '\\u0027', '"': '\\u0022',
                   '>': '\\u003E', '<': '\\u003C', '&': '\\u0026',
                   '=': '\\u003D', '-': '\\u002D', ';': '\\u003B', u'\u2028':
                       '\\u2028', u'\u2029': '\\u2029'}
    _js_escapes.update(('%c' % z, '\\u%04X' % z) for z in xrange(32))
    retval = []
    for letter in value:
        if _js_escapes.has_key(letter):
            retval.append(_js_escapes[letter])
        else:
            retval.append(letter)
    return jinja2.Markup("".join(retval))


# create the jinja template environment
templateLoader = jinja2.FileSystemLoader(searchpath=TEMPLATE_DIR)
templateEnv = jinja2.Environment(loader=templateLoader)
templateEnv.filters['escapejs'] = jinja2_escapejs_filter


def make_demographics():
    """
    Generates the demographic HTML. Since this is constant across workers,
    all that's necessary for it to work is the dictionary of static URLs so
    it knows where to get the jsPsych javascript from.

    :return: The demographics acquisition page HTML.
    """
    demographics = templateEnv.get_template(DEMOGRAPHICS_TEMPLATE)
    return demographics.render(jg=jg)


def make_success():
    """
    Functions just like make_demographics, but returns a 'success page'
    indicating that the task was successfully submitted. It needs
    static_urls for the same reason.

    :return: HTML for a page indicating the worker has successfully completed a
             task.
    """
    success = templateEnv.get_template(SUCCESS_TEMPLATE)
    return success.render(jg=jg)


def make_practice_passed():
    """
    Creates the practice passed success page (see make_success)

    :return: HTML for a page indicating the worker has successfully completed a
             practice.
    """
    success = templateEnv.get_template(SUCCESS_PRACTICE_PASSED_TEMPLATE)
    return success.render(jg=jg)


def make_practice_failed():
    """
    Creates the practice failed page (see make_success)

    :return: HTML for a page indicating the worker has failed a practice.
    """
    success = templateEnv.get_template(SUCCESS_PRACTICE_FAILED_TEMPLATE)
    return success.render(jg=jg)


def make_practice_already_passed():
    """
    Creates the HTML for the case when they are trying to submit a practice
    task but have already passed it.

    :return: HTML page indicating that they cannot re-submit and should
    instead go on to the main task.
    """
    success = templateEnv.get_template(SUCCESS_PRACTICE_ALREADY_PASSED_TEMPLATE)
    return success.render(jg=jg)


def make_html(blocks, task_id=None, box_size=BOX_SIZE, hit_size=HIT_SIZE,
              pos_type=POS_TYPE, attribute=ATTRIBUTE, practice=False,
              collect_demo=False,
              intro_instructions=DEF_INTRO_INSTRUCTIONS):
    """
    Produces an experimental HTML document. By assembling blocks of html code
    into a single html document. Note that this will fill in missing values
    in place!

    INFO
    Similar to JsPsych, this script accepts a list of data structures that
    each represent a portion of the experiment.
    These take the form of dictionaries, called 'blocks':
        block['images'] = a list of lists, where each sublist is of the form
                         [image1, ...].
        block['ims_width'] = a list of lists, identical to block['images'], only
                             in lieu of the filenames these are the image
                             widths.
        block['ims_height'] = a list of lists, identical to block['images'],
                              only in lieu of the filenames these are the image
                              heights.
        block['type'] = 'keep' or 'reject', depending on the trial type. [
                        def: DEF_TRIAL_TYPE]
        block['name'] = The block name. If absent, will be dynamically named.
        block['instructions'] = A list of strings, the instructions to be
                                displayed before the trial begins. This uses
                                the format of JsPsych, i.e., each element of
                                the list is a separate page. Alternatively,
                                each of these may be files that point to
                                jinja templates. [def: DEF_INSTRUCTIONS]
        block['feedback_time'] = an int, how long (in ms) to display
                                 selection feedback. [def: DEF_FEEDBACK_TIME]
        block['trial_time'] = the time for each trial (in ms). [def:
                              DEF_TRIAL_TIME]
        block['response_ends_trial'] = boolean, whether or not a click causes
                                       the trial to advance. [def:
                                       DEF_RESPONSE_ENDS_TRIAL]
        block['prompt'] = a string, the prompt to display during the trial,
                          above the image block. The default prompt  will
                          vary based on whether or not this is a practice
                          task, see the configuration python file.
        block['timing_post_trial'] = numeric, the time that elapses between
                                     each trial (not counting the feedback_time)
                                     in milliseconds.
        block['image_idx_map'] = A list of lists, with the same structure as
                                 'images', but with task-global indices
                                 instead of strings. This is to facilitate
                                 the detection of contradictions under
                                 independent randomization of block types.
        block['global_tup_idxs'] = A list of indices corresponding to the
                                   'identity' index of each tuple. This has a
                                   similar purpose to 'image_idx_map',
                                   in that it maps the shuffled image tuples
                                   back to their "original" order.

    For configuration details, see conf.py.

    :param blocks: These are individual trials, and take the form of
                   dictionaries, called 'blocks'. For the fields, see the
                   readme above.
    :param task_id: The ID of the task, as provided by MTurk
    :param box_size: The size of the images to display in pixels, [w,
                     h]. {def: [800, 800]}
    :param hit_size: The size of the box that contains the images, [w,
                     h]. This is a subbox that will either be (a)
                     centered or (b) randomly positioned. {def: [600, 600]}
    :param pos_type: Either 'random', in which the hit box is placed anywhere
                     inside the box, or 'fixed', where it is centered.
                     [def: 'random']
    :param attribute: The attribute that you want people to judge, e.g.,
                      'interesting'
    :param practice: Boolean. If True, will display the debrief pages.
    :param collect_demo: Boolean. If True, will collect demographic information.
    :param intro_instructions: A </sep>-separated html file containing the
                               instruction templates.
    :return The appropriate HTML for this experiment.
    """
    images = []
    counts = {KEEP_BLOCK: 0, REJECT_BLOCK: 0}
    rblocks = []
    blocknames = []
    if practice:
        p_val = 'true'
    else:
        p_val = 'false'
    if collect_demo:
        d_val = 'true'
    else:
        d_val = 'false'
    if intro_instructions:
        block, start_inst_name = _make_start_block(intro_instructions,
                                                   attribute)
        rblocks.append(block)
        blocknames.append(start_inst_name)
    for n, block in enumerate(blocks):
        # fill in any missing values
        block['type'] = block.get('type', DEF_TRIAL_TYPE)
        try:
            counts[block['type']] += 1
        except:
            raise ValueError("Unknown block type for block %i"%n)
        block['name'] = \
            block.get('name', block['type'] + '_' + str(counts[block['type']]))
        block['instructions'] = block.get('instructions', DEF_INSTRUCTIONS)
        block['feedback_time'] = block.get('feedback_time', DEF_FEEDBACK_TIME)
        block['trial_time'] = block.get('trial_time', DEF_TRIAL_TIME)
        block['prompt'] = block.get('prompt', DEF_PROMPT)
        block['response_ends_trial'] = \
            block.get('response_ends_trial', DEF_RESPONSE_ENDS_TRIAL)
        block['timing_post_trial'] = \
            block.get('timing_post_trial', TIMING_POST_TRIAL)
        block['global_tup_idxs'] = block.get('global_tup_idxs', None)
        block['image_idx_map'] = block.get('image_idx_map', None)
        if block['instructions']:
            # get the filled instruction template
            inst_block, inst_block_name = _make_instr_block(block, attribute)
            rblocks.append(inst_block)
            blocknames.append(inst_block_name)
        # get the filled experimental template
        rblock, rimages = _make_exp_block(block, box_size, hit_size, pos_type)
        blocknames.append(block['name'])
        rblocks.append(rblock)
        images += rimages
    preload = templateEnv.get_template(PRELOAD_TEMPATE)
    base = templateEnv.get_template(BASE_TEMPLATE)
    # fill the preload template
    filled_preload = preload.render(images=images)
    arg_dict = dict()
    arg_dict['blocks'] = rblocks
    arg_dict['preload'] = filled_preload
    arg_dict['blocknames'] = blocknames
    arg_dict['practice'] = p_val
    arg_dict['collect_demo'] = d_val
    arg_dict['taskId'] = str(task_id)
    return base.render(jg=jg, **arg_dict)


def make_practice_limit_html():
    """
    Creates the 'num-practices-exceeded' html.

    NOTES:
        This is now unused, since banning is done implicitly.

    :return: The practices exceeded page HTML.
    """
    template = templateEnv.get_template(PRACTICE_EXCEEDED_TEMPLATE)
    html = template.render(jg=jg)
    return html


def make_error_fetching_task_html():
    """
    Creates a 'error fetching task' page html.

    :return: The error page HTML.
    """
    template = templateEnv.get_template(ERROR_TEMPLATE)
    html = template.render(jg=jg)
    return html


def make_error_submitting_task_html():
    """
    Creates the HTML that indicates there was an error submitting the task.

    :return: The error page HTML.
    """
    template = templateEnv.get_template(ERROR_TEMPLATE)
    html = template.render(jg=jg)
    return html


def _make_exp_block(block, box_size, hit_size, pos_type):
    """
    Accepts a block dict (see readme) and returns an appropriate experimental
    block, which consists of sequential images to be presented to the mturk
    worker.

    :param block: A dictionary that defines a block; see the readme.
    :param box_size: The box size, see make()
    :param hit_size: The hit box size, see make()
    :param pos_type: How to position the hit box in the box, see make()
    :return: An experimental block, a dictionary that can be used to fill the
             experimental template. Additionally returns a list of images
             involved in this block, which can be used for image preloading.
    """
    rblock = dict()
    rblock['stimset'] = []
    images = []
    for stimuli, widths, heights in zip(block['images'], block['ims_width'],
                                        block['ims_height']):
        if pos_type == 'random':
            rx = np.random.randint(0, box_size[0] - hit_size[0])
            ry = np.random.randint(0, box_size[1] - hit_size[1])
        else:
            rx = int((box_size[0] - hit_size[0]) / 2.)
            ry = int((box_size[1] - hit_size[1]) / 2.)
        dstimuli = [[x, y, z] for x, y, z in zip(stimuli, widths, heights)]
        cstimuli = _fit_images(dstimuli, hit_size)
        for stimulus in cstimuli:
            images.append(stimulus['file'])
            stimulus['id'] = stimulus['file'].split('/')[-1].split('.')[0]
            stimulus['x'] += rx
            stimulus['y'] += ry
        rblock['stimset'].append(cstimuli)
    rblock['type'] = block['type']
    rblock['name'] = block['name']
    rblock['feedback_time'] = block['feedback_time']
    rblock['trial_time'] = block['trial_time']
    rblock['response_ends_trial'] = block['response_ends_trial']
    rblock['prompt'] = block['prompt']
    rblock['timing_post_trial'] = block['timing_post_trial']
    rblock['global_tup_idxs'] = block['global_tup_idxs']
    rblock['image_idx_map'] = block['image_idx_map']
    template = templateEnv.get_template(TRIAL_BLOCK_TEMPLATE)
    filled_template = template.render(block=rblock)
    return filled_template, images


def _fit_images(images, hit_size):
    """
    Computes the x- and y-positions for a list of images and their widths and
    heights such that the following constraints are obeyed:
        - No image exceeds the height of the hit box.
        - Every image has an equal size.
        - The images, laid side-by-side, occupy as much of the hit box's
          width as possible.

    :param images: A list of the form [[filename, width, height], ...]
                   filenames or URLs to [width, height] tuples.
    :param hit_size: The size of the hitbox, see make()
    :return: A list of dictionaries with fields (x, y, width, height) tuples.
    """
    # gets the sizes for each image in [w, h]
    im_dims = [n[1, 2] for n in images]
    images = [n[0] for n in images]
    for n in range(len(im_dims)):
        if im_dims[n][0] is None or im_dims[n][1] is None:
            im_dims[n] = _get_im_dims(images[n])
    # compute the maximum area, scale each image up so they're equal
    max_area = max([x*y for x, y in im_dims])
    for idx in range(len(im_dims)):
        x, y = im_dims[idx]
        area_ratio = float(max_area) / (x*y)
        x *= np.sqrt(area_ratio)
        y *= np.sqrt(area_ratio)
        im_dims[idx] = [x, y]
    width_sum = np.sum([x[0] for x in im_dims]) + MARGIN_SIZE * (len(im_dims)
                                                                 * 2 + 2)
    width_ratio = float(hit_size[0]) / width_sum
    for idx in range(len(im_dims)):
        x, y = im_dims[idx]
        x *= width_ratio
        y *= width_ratio
        im_dims[idx] = [x, y]
    max_height = max([x[1] for x in im_dims]) + 2 * MARGIN_SIZE
    if max_height > hit_size[1]:
        # resize them a third time if at least one image is too tall beware
        # of a slight numerical error that can occu r here, do not calculate
        # height ratio based on max_height since that has the margin size
        # added in, but this is not modified by the ratio multiplier. The
        # extra -1 is to prevent it from being numerically too close
        height_ratio = \
            float(hit_size[1] - 2 * MARGIN_SIZE - 1) / \
            max([x[1] for x in im_dims])
        for idx in range(len(im_dims)):
            x, y = im_dims[idx]
            x *= height_ratio
            y *= height_ratio
            im_dims[idx] = [x, y]
        max_height = max([x[1] for x in im_dims]) + 2 * MARGIN_SIZE
    cur_x_pos = MARGIN_SIZE # current x position
    res = [] # the results
    # compute the height offset
    height_offset = np.random.randint(0, hit_size[1] - max_height)
    for n, (x, y) in enumerate(im_dims):
        cdict = dict()
        cdict['x'] = cur_x_pos
        cdict['width'] = int(x)
        cdict['height'] = int(y)
        cdict['y'] = int(float(max_height) / 2 - float(y) / 2) + height_offset
        cdict['file'] = images[n]
        cur_x_pos += cdict['width'] + 2 * MARGIN_SIZE
        res.append(cdict)
    return res


def _get_im_dims(image):
    """
    Returns the dimensions of an image file in pixels as [width, height].
    This is unfortunately somewhat time consuming as the images have to be
    loaded in order to determine their dimensions. Its likely that this can
    be accomplished in pure javascript, however I (a) don't know enough
    javascript and (b) want more explicit control.

    :param image: The filename or URL of an image.
    :return: A list, the dimensions of the image in pixels, as [width, height].
    """
    file = cStringIO.StringIO(urllib.urlopen(image).read())
    im = Image.open(file)
    width, height = im.size
    return [width, height]


def _make_instr_block(block, attribute):
    """
    Accepts a block dict (see readme) and returns an appropriate instruction
    block.

    :param block: A dictionary that defines a block; see the readme.
    :return: Instruction block, a dictionary that can be used to fill the
             instruction template, and the instruction block name.
    """
    rblock = dict()
    rblock['name'] = block['name'] + '_instr'
    rblock['instructions'] = [_create_instruction_page(x, attribute) for x in
                              block['instructions']]
    template = templateEnv.get_template(INSTRUCTION_TEMPLATE)
    filled_template = template.render(block=rblock, attribute=attribute)
    return filled_template, rblock['name']


def _create_instruction_page(instruction, attribute):
    """
    Creates a single instruction page, and attempts to intelligently define
    the items that require filling. Note that if a template file (or one that
    does not exist) is not provided, then it will simply return whatever is
    passed in as the instructions.

    :param instruction: The template filename, or a string that will be the
                        instructions.
    :param attribute: The study attribute.
    :return: The instruction page as a filled template.
    """
    try:
        template_class = templateEnv.get_template(instruction)
    except:
        return instruction
    filled_template = template_class.render(jg=jg)
    # perform replacements
    # must eliminate the carriage returns, quotes
    filled_template = filled_template.replace('\n', '')
    filled_template = filled_template.replace('"', '\"')
    filled_template = filled_template.replace("'", "\'")
    return filled_template


def _make_start_block(intro_instructions, attribute):
    """
    Accepts the attribute that we will be scoring, a sequence of instruction
    templates, and converts them into an instruction block.

    :param intro_instructions: A </sep>-separated html file containing the
                               instruction templates.
    :param attribute: The attribute that will be scored.
    :return: An instruction block as a filled template.
    """
    template_class = templateEnv.get_template(intro_instructions)
    filled_template = template_class.render(jg=jg)
    pages = filled_template.split('</sep>')
    pages = filter(lambda x: len(x), pages)
    block = {'instructions': pages, 'name': 'start_block'}
    template = templateEnv.get_template('inst_template.html')
    filled_template = template.render(block=block)
    return filled_template, 'start_block'


def _get_template_variables(template):
    """
    Accepts a template file, returns the undeclared variables.

    :param template: A jinja2 template filename, from which we will extract
                     the variables.
    :return: The undeclared variables (i.e., those that still need to be
             defined) as a set of strings.
    """
    src = templateEnv.loader.get_source(templateEnv, template)
    vars = meta.find_undeclared_variables(templateEnv.parse(src))
    return vars


def make_preview_page(is_practice=False, task_time=None):
    """
    Returns the HTML for a 'preview' page, which users are presented when
    they begin to preview a task.

    :param is_practice: A boolean indicating whether or not this is a practice.
    :param task_time: The estimated amount of time the task will take, in ms.
    :return: HTML for the preview page.
    """
    if is_practice:
        template = templateEnv.get_template(PRACTICE_PREVIEW_TEMPLATE)
    else:
        template = templateEnv.get_template(PREVIEW_TEMPLATE)
    if task_time is None:
        task_time = 'Unknown'
    else:
        mins, secs = divmod(task_time, 60)
        if not mins:
            task_time = '%i seconds' % secs
        else:
            task_time = '%i minutes and %i seconds' % (mins, secs)
    return template.render(task_time=task_time, jg=jg)


def _make_preview_page_dep(mt, worker_id, is_practice=False):
    """
    ** DEPRICATED **
    ** We can't get the worker ID from previews, so none of this is possible. **

    Returns the HTML for a 'preview' page, which users are presented when
    they begin to preview a task. If this is *the first time* a worker is
    previewing one of our practice tasks, then it will assign them a practice
    quota without them having to do anything.

    :param mt: The mechanical turk API object.
    :param worker_id: The worker ID, as provided by MTurk
    :param is_practice: A boolean indicating whether or not this is a practice.
    :return: HTML for the preview page.
    """
    if is_practice:
        if mt.get_worker_passed_practice(worker_id):
            template = templateEnv.get_template(
                    PRACTICE_PREVIEW_ALREADY_PASSED)
        else:
            practice_quota = mt.get_worker_avail_practice(worker_id)
            if practice_quota is None:
                mt.reset_worker_weekly_practice_quota(worker_id)
                template = templateEnv.get_template(PRACTICE_PREVIEW_TEMPLATE)
            elif practice_quota <= 0:
                template = templateEnv.get_template(
                    PRACTICE_PREVIEW_QUOTA_EXCEEDED)
            else:
                template = templateEnv.get_template(PRACTICE_PREVIEW_TEMPLATE)
    else:
        if not mt.get_worker_passed_practice(worker_id):
            template = templateEnv.get_template(PREVIEW_TEMPLATE_NEED_PRACTICE)
        else:
            task_quota = mt.get_worker_avail_tasks(worker_id)
            if task_quota is None or task_quota <= 0:
                template = templateEnv.get_template(PREVIEW_TOO_MANY_TASKS)
            else:
                template = templateEnv.get_template(PREVIEW_TEMPLATE)
    return template.render(jg=jg)


def fetch_task(dbget, dbset, task_id, worker_id=None):
    """
    Constructs a task after a request hits the webserver. In contrast to
    build_task, this is for requests that have a task ID encoded in
    them--i.e., the request is for a specific task. It does not check if the
    worker is banned or if they need a practice instead of a normal task.
    Instead, these data are presumed to be encoded in the MTurk structure.

    NOTES:
        'build_task' is a relic of an earlier iteration,
        in mt2_generate.request_for_task.

    :param dbget: An instance of db.Get
    :param dbset: An instance of db.Set.
    :param task_id: The task ID, as a string.
    :param worker_id: The worker ID, as a string. If this is a preview,
                      you cannot obtain the worker ID, thus supply None.
    :return: The HTML for the requested task.
    """
    # check that the worker exists, else register them. We want to have their
    #  information in the database so we don't spawn errors down the road.
    if not dbget.worker_exists(worker_id):
        dbset.register_worker(worker_id)
    # check if we need demographics or not
    is_practice = dbget.task_is_practice(task_id)
    collect_demo = False
    if is_practice:
        intro_instructions = DEF_PRACTICE_INTO_INSTRUCTIONS
    else:
        intro_instructions = DEF_INTRO_INSTRUCTIONS
    if is_practice:
        if dbget.worker_need_demographics(worker_id):
            collect_demo = True
        dbset.practice_served(task_id, worker_id)
    else:
        dbset.task_served(task_id, worker_id)
    blocks = dbget.get_task_blocks(task_id)
    _log.debug('Fetched task blocks for task %s' % task_id)
    if blocks is None:
        # display an error-fetching-task page.
        _log.warn('Could not fetch blocks for task %s' % task_id)
        return make_error_fetching_task_html()
    _log.debug('Assembling HTML')
    html = make_html(blocks,
                     practice=is_practice,
                     collect_demo=collect_demo,
                     intro_instructions=intro_instructions,
                     task_id=task_id)
    _log.debug('HTML assembly finished')
    if not is_practice:
        # this is now disabled
        # dbset.set_task_html(task_id, html)
        pass
    return html

