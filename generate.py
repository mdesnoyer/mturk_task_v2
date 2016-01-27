"""
Exports functions specific to integrating information from MTurk and the database to generate the task HTML itself.
The external resources in ./resources are specifically in the purview of generate.py.
"""

from conf import *
import urllib
import cStringIO
from PIL import Image
import numpy as np
import os
import jinja2
from jinja2 import meta


# create the jinja escape filter
def jinja2_escapejs_filter(value):
    # Perform Jinja2 Escapes
    _js_escapes = {'\\': '\\u005C', '\'': '\\u0027', '"': '\\u0022', '>': '\\u003E', '<': '\\u003C', '&': '\\u0026',
                   '=': '\\u003D', '-': '\\u002D', ';': '\\u003B', u'\u2028': '\\u2028', u'\u2029': '\\u2029'}
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


def make_html(blocks, task_id=None, preload_images=PRELOAD_IMAGES, box_size=BOX_SIZE, hit_size=HIT_SIZE,
              pos_type=POS_TYPE, attribute=ATTRIBUTE, instruction_sequence=TASK_INSTRUCTION_SEQUENCE, practice=False,
              collect_demo=False, is_preview=False):
    """
    Produces an experimental HTML document. By assembling blocks of html code into a single html document. Note that
    this will fill in missing values in place!

    INFO
    Similar to JsPsych, this script accepts a list of data structures that each represent a portion of the experiment.
    These take the form of dictionaries, called 'blocks':
        block['images'] = a list of lists, where each sublist is of the form [image1, ...].
        block['type'] = 'keep' or 'reject', depending on the trial type. [def: DEF_TRIAL_TYPE]
        block['name'] = The block name. If absent, will be dynamically named.
        block['instructions'] = A list of strings, the instructions to be displayed before the trial begins. This uses
                                the format of JsPsych, i.e., each element of the list is a separate page. Alternatively,
                                each of these may be files that point to jinja templates. [def: DEF_INSTRUCTIONS]
        block['feedback_time'] = an int, how long (in ms) to display selection feedback. [def: DEF_FEEDBACK_TIME]
        block['trial_time'] = the time for each trial (in ms). [def: DEF_TRIAL_TIME]
        block['response_ends_trial'] = boolean, whether or not a click causes the trial to advance.
                                       [def: DEF_RESPONSE_ENDS_TRIAL]
        block['prompt'] = a string, the prompt to display during the trial, above the image block. The default prompt
                          will vary based on whether or not this is a practice task, see the configuration python file.

    For configuration details, see conf.py.

    :param blocks: These are individual trials, and take the form of dictionaries, called 'blocks'. For the fields, see
                   the readme above.
    :param task_id: The ID of the task, as provided by MTurk
    :param preload_images: Boolean, whether or not to fetch images ahead of time. [def: True]
    :param box_size: The size of the images to display in pixels, [w, h]. {def: [800, 800]}
    :param hit_size: The size of the box that contains the images, [w, h]. This is a subbox that will either be (a)
                     centered or (b) randomly positioned. {def: [600, 600]}
    :param pos_type: Either 'random', in which the hit box is placed anywhere inside the box, or 'fixed', where it is
                     centered. [def: 'random']
    :param attribute: The attribute that you want people to judge, e.g., 'interesting'
    :param instruction_sequence: The sequence of instruction pages to display at the outset of the experiment.
    :param practice: Boolean. If True, will display the debrief pages.
    :param collect_demo: Boolean. If True, will collect demographic information.
    :param is_preview: Boolean. If True, will assume that this is a preview of the experiment and only render the
                       instructions. This should only be true if the worker is previewing the HIT, i.e., the value
                       of assignmentId is ASSIGNMENT_ID_NOT_AVAILABLE
    :return The appropriate HTML for this experiment.
    """
    # TODO: Make sure this thing uses task_id and preload_images!
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
    if instruction_sequence:
        block, start_inst_name = _make_start_block(instruction_sequence, attribute)
        rblocks.append(block)
        blocknames.append(start_inst_name)
    if is_preview:
        base = templateEnv.get_template(BASE_TEMPLATE)
        html = base.render(blocks=rblocks, blocknames=blocknames, attribute='None',
                           practice='false', collect_demo='false', taskId='preview')
        return html
    for n, block in enumerate(blocks):
        # fill in any missing values
        block['type'] = block.get('type', DEF_TRIAL_TYPE)
        try:
            counts[block['type']] += 1
        except:
            raise ValueError("Unknown block type for block %i"%n)
        block['name'] = block.get('name', block['type'] + '_' + str(counts[block['type']]))
        block['instructions'] = block.get('instructions', DEF_INSTRUCTIONS)
        block['feedback_time'] = block.get('feedback_time', DEF_FEEDBACK_TIME)
        block['trial_time'] = block.get('trial_time', DEF_TRIAL_TIME)
        block['prompt'] = block.get('prompt', DEF_PROMPT)
        block['response_ends_trial'] = block.get('response_ends_trial', DEF_RESPONSE_ENDS_TRIAL)
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
    html = base.render(blocks=rblocks, preload=filled_preload, blocknames=blocknames, attribute=attribute,
                       practice=p_val, collect_demo=d_val, taskId=str(task_id))
    return html


def make_ban_html(dbget, worker_id):
    """
    Creates the 'you are banned' html.

    :param dbget: An instance of db.Get
    :param worker_id: The worker ID, as a string.
    :return: The ban page HTML.
    """
    reason, rem_dur = dbget.get_worker_ban_time_reason(worker_id)
    template = templateEnv.get_template(BAN_TEMPLATE)
    html = template.render(ban_time=rem_dur, ban_reason=reason)
    return html


def make_practice_limit_html(workerId):
    """
    Creates the 'num-practices-exceeded' html.

    :param workerId: The worker ID, as a string.
    :return: The practices exceeded page HTML.
    """
    template = templateEnv.get_template(PRACTICE_EXCEEDED_TEMPLATE)
    html = template.render()
    return html


def make_no_avail_tasks_html(workerId):
    """
    Creates the 'no tasks are available' html.

    NOTE:
        For now, we will be using make_error_fetching_task_html as the page for the no-tasks-available situation.

    :param workerId: The worker ID, as a string.
    :return: The no tasks page HTML.
    """
    raise NotImplementedError()


def make_error_fetching_task_html(workerId):
    """
    Creates a 'error fetching task' page html.

    :param workerId: The worker ID, as a string.
    :return: The error page HTML.
    """
    template = templateEnv.get_template(ERROR_TEMPLATE)
    html = template.render()
    return html


def _make_exp_block(block, box_size, hit_size, pos_type):
    """
    Accepts a block dict (see readme) and returns an appropriate experimental block, which consists of sequential images
    to be presented to the mturk worker.

    :param block: A dictionary that defines a block; see the readme.
    :param box_size: The box size, see make()
    :param hit_size: The hit box size, see make()
    :param pos_type: How to position the hit box in the box, see make()
    :return: An experimental block, a dictionary that can be used to fill the experimental template. Additionally
    returns a list of images involved in this block, which can be used for image preloading.
    """
    rblock = dict()
    rblock['stimset'] = []
    images = []
    for stimuli in block['images']:
        if pos_type == 'random':
            rx = np.random.randint(0, box_size[0] - hit_size[0])
            ry = np.random.randint(0, box_size[1] - hit_size[1])
        else:
            rx = int((box_size[0] - hit_size[0]) / 2.)
            ry = int((box_size[1] - hit_size[1]) / 2.)
        cstimuli = _fit_images(stimuli, hit_size)
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
    template = templateEnv.get_template(TRIAL_BLOCK_TEMPLATE)
    filled_template = template.render(block=rblock)
    return filled_template, images


def _fit_images(images, hit_size):
    """
    Computes the x- and y-positions for a list of images and their widths and heights such that the following
    constraints are obeyed:
        - No image exceeds the height of the hit box.
        - Every image has an equal size.
        - The images, laid side-by-side, occupy as much of the hit box's width as possible.

    :param images: A list of image filenames or URLs.
    :param hit_size: The size of the hitbox, see make()
    :return: A list of dictionaries with fields (x, y, width, height) tuples.
    """
    im_dims = [_get_im_dims(im) for im in images] # gets the sizes for each image in [w, h]
    max_area = max([x*y for x, y in im_dims]) # compute the maximum area, scale each image up so they're equal
    for idx in range(len(im_dims)):
        x, y = im_dims[idx]
        area_ratio = float(max_area) / (x*y)
        x *= np.sqrt(area_ratio)
        y *= np.sqrt(area_ratio)
        im_dims[idx] = [x, y]
    width_sum = np.sum([x[0] for x in im_dims]) + MARGIN_SIZE * (len(im_dims) * 2 + 2)
    width_ratio = float(hit_size[0]) / width_sum
    for idx in range(len(im_dims)):
        x, y = im_dims[idx]
        x *= width_ratio
        y *= width_ratio
        im_dims[idx] = [x, y]
    max_height = max([x[1] for x in im_dims]) + 2 * MARGIN_SIZE
    if max_height > hit_size[1]: # resize them a third time if at least one image is too tall
        height_ratio = float(hit_size[1]) / max_height
        for idx in range(len(im_dims)):
            x, y = im_dims[idx]
            x *= height_ratio
            y *= height_ratio
            im_dims[idx] = [x, y]
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
    Returns the dimensions of an image file in pixels as [width, height]. This is unfortunately somewhat time
    consuming as the images have to be loaded in order to determine their dimensions. Its likely that this can be
    accomplished in pure javascript, however I (a) don't know enough javascript and (b) want more explicit control.

    :param image: The filename or URL of an image.
    :return: A list, the dimensions of the image in pixels, as [width, height].
    """
    file = cStringIO.StringIO(urllib.urlopen(image).read())
    im = Image.open(file)
    width, height = im.size
    return [width, height]


def _make_instr_block(block, attribute):
    """
    Accepts a block dict (see readme) and returns an appropriate instruction block.

    :param block: A dictionary that defines a block; see the readme.
    :return: Instruction block, a dictionary that can be used to fill the instruction template, and the instruction
    block name.
    """
    rblock = dict()
    rblock['name'] = block['name'] + '_instr'
    rblock['instructions'] = [_create_instruction_page(x, attribute) for x in block['instructions']]
    template = templateEnv.get_template(INSTRUCTION_TEMPLATE)
    filled_template = template.render(block=rblock, attribute=attribute)
    return filled_template, rblock['name']


def _create_instruction_page(instruction, attribute):
    """
    Creates a single instruction page, and attempts to intelligently define the items that require filling. Note that
    if a template file (or one that does not exist) is not provided, then it will simply return whatever is passed in
    as the instructions.

    :param template: The template filename, or a string that will be the instructions.
    :param attribute: The study attribute.
    :return: The instruction page as a filled template.
    """
    try:
        template_class = templateEnv.get_template(instruction)
    except:
        return instruction
    vars = _get_template_variables(instruction)
    var_dict = dict()
    # create a dict of variables to set in the template
    if 'attribute' in vars:
        var_dict['attribute'] = attribute
    if 'image_dir' in vars:
        var_dict['image_dir'] = os.path.join(ROOT, PRACTICE_IM_DIR)
    filled_template = template_class.render(**var_dict)
    # perform replacements
    filled_template = filled_template.replace('\n', '')  # must eliminate the carriage returns!
    filled_template = filled_template.replace('"', '\"')  # escape quotes!
    filled_template = filled_template.replace("'", "\'")  # escape quotes!
    return filled_template


def _make_start_block(instruction_sequence, attribute):
    """
    Accepts the attribute that we will be scoring, a sequence of instruction templates, and converts them into an
    instruction block.

    :param instruction_sequence: A list of filenames, in order, which points to template files.
    :param attribute: The attribute that will be scored.
    :return: An instruction block as a filled template.
    """
    pages = []
    for ist in instruction_sequence: # ist = instruction template
        pages.append(_create_instruction_page(ist, attribute))
    block = {'instructions':pages, 'name': 'start_block'}
    template = templateEnv.get_template('inst_template.html')
    filled_template = template.render(block=block)
    return filled_template, 'start_block'


def _get_template_variables(template):
    """
    Accepts a template file, returns the undeclared variables.

    :param template: A template file, from which we will extract the variables.
    :return: The undeclared variables (i.e., those that still need to be defined) as a set
    """
    src = templateEnv.loader.get_source(templateEnv, template)
    vars = meta.find_undeclared_variables(templateEnv.parse(src))
    return vars


def fetch_task(dbget, dbset, worker_id, task_id, is_preview=False):
    """
    Constructs a task after a request hits the webserver. In contrast to build_task, this is for requests that have a
    task ID encoded in them--i.e., the request is for a specific task. It does not check if the worker is banned or if
    they need a practice instead of a normal task. Instead, these data are presumed to be encoded in the MTurk
    structure.

    NOTES:
        'build_task' is a relic of an earlier iteration, in mt2_generate.request_for_task.

    :param dbget: An instance of db.Get
    :param dbset: An instance of db.Set.
    :param worker_id: The worker ID, as a string.
    :param task_id: The task ID, as a string.
    :param is_preview: The task to be served up is a 'preview' task.
    :return: The HTML for the requested task.
    """
    # check that the worker exists, else register them. We want to have their information in the database so we don't
    # spawn errors down the road.
    if not dbget.worker_exists(worker_id):
        dbset.register_worker(worker_id)
    # check if we need demographics or not
    is_practice = dbget.task_is_practice(task_id)
    collect_demo = False
    if dbget.worker_need_demographics(worker_id):
        collect_demo = True
    if not is_preview:
        if is_practice:
            dbset.practice_served(task_id, worker_id)
        else:
            dbset.task_served(task_id, worker_id)
        blocks = dbget.get_task_blocks(task_id)
        if blocks is None:
            # display an error-fetching-task page.
            return make_error_fetching_task_html(worker_id)
    else:
        blocks = []  # do not show them anything if this is just a preview.
    html = make_html(blocks, practice=is_practice, collect_demo=collect_demo, is_preview=is_preview)
    # TODO: add the html to the database
    if not is_practice:
        dbset.set_task_html(task_id, html)
    return html

