"""
Generates an html file for an experiment, using jsPsych and the custom plugin I created, click-choice.

Similar to JsPsych, this script accepts a list of data structures that each represent a portion of the experiment.
These take the form of dictionaries, called 'blocks':
    block['images'] = a list of lists, where each sublist is of the form [image1, ...].
    block['type'] = 'accept' or 'reject', depending on the trial type. [def: 'accept']
    block['name'] = The block name. If absent, will be dynamically named.
    block['instructions'] = A list of strings, the instructions to be displayed before the trial begins. This uses the
                            format of JsPsych, i.e., each element of the list is a separate page. Alternatively, each
                            of these may be files that point to jinja templates. [def: None]
    block['feedback_time'] = an int, how long (in ms) to display selection feedback. [def: 100]
    block['trial_time'] = the time for each trial (in ms). [def: 1500]
    block['response_ends_trial'] = boolean, whether or not a click causes the trial to advance. [def: True]
    block['prompt'] = a string, the prompt to display during the trial, above the image block. [def: ''] (this should
                      be used for testing).

Additionally, there are a few global parameters:
    preload_images = boolean, whether or not to fetch images ahead of time. [def: True]
    box_size = the size of the images to display in pixels, [w, h]. {def: [800, 800]}
    hit_size = the size of the box that contains the images, [w, h]. This is a subbox that will either be (a) centered
               or (b) randomly positioned. {def: [600, 600]}
    pos_type = either random or fixed, see block()
"""

import urllib, cStringIO
from PIL import Image
import numpy as np
import os
import jinja2
from jinja2 import meta

# set the defaults
DEF_FEEDBACK_TIME = 100
DEF_TRIAL_TIME = 1500
DEF_INSTRUCTIONS = None
DEF_RESPONSE_ENDS_TRIAL = 'true'
DEF_TRIAL_TYPE = 'keep'
DEF_PROMPT = ''

# set the templates to be used
BASE_TEMPLATE = 'base.html'
PRELOAD_TEMPATE = 'preload_template.html'
INSTRUCTION_TEMPLATE = 'inst_template.html'
TRIAL_BLOCK_TEMPLATE = 'trial_block_template.html'
PRACTICE_IM_DIR = 'instr_screenshots/'

# formatting defaults
MARGIN_SIZE = 2

# determine the root of the templates
ROOT = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_DIR = os.path.join(ROOT, 'templates/')

# create the jinja template environment
templateLoader = jinja2.FileSystemLoader(searchpath=TEMPLATE_DIR)
templateEnv = jinja2.Environment(loader=templateLoader)

# --------------------------------
# Perform Jinja2 Escapes
_js_escapes = {
        '\\': '\\u005C',
        '\'': '\\u0027',
        '"': '\\u0022',
        '>': '\\u003E',
        '<': '\\u003C',
        '&': '\\u0026',
        '=': '\\u003D',
        '-': '\\u002D',
        ';': '\\u003B',
        u'\u2028': '\\u2028',
        u'\u2029': '\\u2029'
}
# Escape every ASCII character with a value less than 32.
_js_escapes.update(('%c' % z, '\\u%04X' % z) for z in xrange(32))
def jinja2_escapejs_filter(value):
        retval = []
        for letter in value:
                if _js_escapes.has_key(letter):
                        retval.append(_js_escapes[letter])
                else:
                        retval.append(letter)

        return jinja2.Markup("".join(retval))
templateEnv.filters['escapejs'] = jinja2_escapejs_filter
# --------------------------------

def make(blocks, preload_images=True, box_size=[800, 500], hit_size=[600, 400], pos_type='random',
         attribute='interesting', instruction_sequence=[], practice=False):
    """
    Produces an experimental HTML document. By assembling blocks of html code into a single html document. Note that
    this will fill in missing values in place!

    :param blocks: These are individual trials, and take the form of dictionaries, called 'blocks'. For the fields, see
                   the readme above.
    :param preload_images: Boolean, whether or not to fetch images ahead of time. [def: True]
    :param box_size: The size of the images to display in pixels, [w, h]. {def: [800, 800]}
    :param hit_size: The size of the box that contains the images, [w, h]. This is a subbox that will either be (a)
                     centered or (b) randomly positioned. {def: [600, 600]}
    :param pos_type: Either 'random', in which the hit box is placed anywhere inside the box, or 'fixed', where it is
                     centered. [def: 'random']
    :param attribute: The attribute that you want people to judge, e.g., 'interesting'
    :param instruction_sequence: The sequence of instruction pages to display at the outset of the experiment.
    :param practice: Boolean. If True, will display (a) the demographics form and (b) the the debrief.
    :return The appropriate HTML for this experiment.
    """
    images = []
    counts = {'keep': 0, 'reject': 0}
    rblocks = []
    blocknames = []
    if practice:
        p_val = 'true'
    else:
        p_val = 'false'
    if instruction_sequence:
        block, start_inst_name = _make_start_block(instruction_sequence, attribute)
        rblocks.append(block)
        blocknames.append(start_inst_name)
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
                       practice=p_val)
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
    filled_template = template_class.render(**var_dict)\
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