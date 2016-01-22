"""
This script exports functions for dealing with the types of MTurk transactions. This is designed to (a) allow us to
control what kinds of tasks are being presented to workers, and also to manage the state of our experiment on MTurk
much in the same manner as database.get/set.
"""

from conf import *
import boto
import hashlib

"""
FUNCTIONS WE WILL NEED:

(1) Add task awaiting HIT to HIT Group
(2) Check if uncompleted task is in HIT Group
(3) Ban worker (change status) (add temporary ban qualification)
(4) Unban worker (change status)
(5) Approve worker (change status) (add practice passed)
(6) Unapprove worker (change status)
"""


def _gen_qualification(task_type='REAL'):
    """
    Obtains a qualification object.

    :param task_type: Either 'practice' or something else. If 'practice,' will return the qualifications for a practice.
    :return: A boto mturk qualification object.
    """
    if type(task_type) is str:
        if task_type.lower() == 'practice':
            return boto.mturk.qualification.Requirement(QUALIFICATION_ID, 'DoesNotExist')
    return boto.mturk.qualification.Requirement(QUALIFICATION_ID, 'EqualTo', PASSED_PRACTICE, required_to_preview=True)


def grant_worker_practice_passed(mtconn, worker_id):
    """
    Grants worker the qualification necessary to begin attempting to complete our real tasks.

    :param mtconn: The Boto mechanical turk connection object.
    :param worker_id: The MTurk worker ID.
    :return: None
    """
    # TODO: implement
    raise NotImplementedError()


def _revoke_worker_practice_passed(mtconn, worker_id):
    """
    Revokes the qualification necessary to begin attempting to complete our real tasks from a worker.

    :param mtconn: The Boto mechanical turk connection object.
    :param worker_id: The MTurk worker ID.
    :return: None
    """
    # TODO: implement
    raise NotImplementedError()


def ban_worker(mtconn, worker_id, reason=DEFAULT_BAN_REASON):
    """
    Bans a worker. Also revokes their qualification.

    :param mtconn: The Boto mechanical turk connection object.
    :param worker_id: The MTurk worker ID.
    :param reason: The reason for the ban.
    :return: None
    """
    # TODO: implement
    raise NotImplementedError()


def unban_worker(mtconn, worker_id):
    """
    Un-bans a worker.

    :param mtconn: The Boto mechanical turk connection object.
    :param worker_id: The MTurk worker ID.
    :return: None
    """
    # TODO: implement
    raise NotImplementedError()


def get_hit_complete(mtconn, hit_id=None):
    """
    Determines if a task has been completed or not.

    :param mtconn: The Boto mechanical turk connection object.
    :param hit_id: The ID of the hit in question.
    :return: True if the task/HIT has been completed, otherwise false.
    """
    # TODO: implement
    raise NotImplementedError()


def register_hit_type_mturk(conn, mtconn, task_attribute=ATTRIBUTE, image_attributes=IMAGE_ATTRIBUTES,
                            title=DEFAULT_TASK_NAME, practice_title=DEFAULT_PRACTICE_TASK_NAME,
                            description=DESCRIPTION, practice_description=PRACTICE_DESCRIPTION,
                            reward=DEFAULT_TASK_PAYMENT, practice_reward=DEFAULT_PRACTICE_PAYMENT,
                            assignment_duration=ASSIGNMENT_DURATION, keywords=KEYWORDS,
                            auto_approve_delay=AUTO_APPROVE_DELAY, active=TRUE):
    """
    Registers a new HIT type.

    NOTES:

        ** THIS FUNCTION MODIFIES THE DATABASE

        The idea of practices and 'true' tasks is abstracted away here. It is assumed that for every HIT type, there
        will be defined practice and true tasks. Hence:
            ! THIS FUNCTION CREATES BOTH A PRACTICE AND A TRUE HIT TYPE ID BOTH ON MTURK
            AND IN THE DATABASE !

    :param conn: The HappyBase connection object.
    :param mtconn: The Boto mechanical turk connection object.
    :param task_attribute: The task attribute for tasks that are HITs assigned to this HIT type.
    :param image_attributes: The image attributes for tasks that are HITs assigned to this HIT type.
    :param title: The HIT type title.
    :param practice_title: The HIT type title for practices.
    :param description: The HIT Type description.
    :param practice_description: The HIT type description to accompany practice tasks.
    :param reward: The reward for completing this type of HIT.
    :param practice_reward: The reward for completing practices of this HIT type.
    :param assignment_duration: How long this HIT type persists for.
    :param keywords: The HIT type keywords.
    :param auto_approve_delay: The auto-approve delay.
    :param active: FALSE or TRUE (see conf). Whether or not this HIT is active, i.e., if new HITs / Tasks should be
                   assigned to this HIT type.
    :return: If successful, returns nothing. Else returns the hit type registration object on the first failed attempt
             (either for creating the practice HIT type or the true HIT type).
    """
    true_opobj = dict()
    prac_opobj = dict()

    # define the operation object for the 'true' task HIT type ID
    true_opobj['title'] = title
    true_opobj['description'] = description
    true_opobj['reward'] = reward
    true_opobj['duration'] = assignment_duration
    true_opobj['keywords'] = keywords
    true_opobj['approval_delay'] = auto_approve_delay
    true_opobj['qual_req'] = _gen_qualification()

    # define the operation object for the practice task HIT type ID
    prac_opobj['title'] = practice_title
    prac_opobj['description'] = practice_description
    prac_opobj['reward'] = practice_reward
    prac_opobj['duration'] = assignment_duration
    prac_opobj['keywords'] = keywords
    prac_opobj['approval_delay'] = auto_approve_delay
    # TODO: implement
    raise NotImplementedError()


def add_hit_to_hit_type(conn, mtconn, hit_type_id, task_id, is_practice=False, reward=None):
    """
    Creates a mechanical turk task, and assigns it to the HIT type specified.

    NOTES:

        ** THIS FUNCTION MODIFIES THE DATABASE **

    :param conn: The HappyBase connection object.
    :param mtconn: The Boto mechanical turk connection object.
    :param hit_type_id: A string, specifying the HIT type using its ID.
    :param task_id: The task ID that this HIT will serve up.
    :param is_practice: Boolean, indicating whether or not this task is a practice.
    :param reward: The reward amount (as a float).
    :return: On success None, on failure the HIT Creation response object.
    """
    # TODO: Verify that the hit type is_practice corresponds to the value of is_practice?
    # TODO: meta to-do: if we don't institute these checks below, remove 'conn' from the list of arguments.
    # TODO: check that the task ID exists in the database?
    # TODO: check that the task type ID exists in the database?
    # TODO: Figure out why there isn't an AssignmentReviewPolicy in boto...
    question_object = boto.mturk.question.ExternalQuestion(external_url=EXTERNAL_QUESTION_ENDPOINT,
                                                           frame_height=BOX_SIZE[1])
    # TODO: Finish this!
    opobj = dict()
    opobj['hit_type'] = hit_type_id
    opobj['question'] = question_object
    opobj['lifetime'] = HIT_LIFETIME_IN_SECONDS
    opobj['max_assignment'] = 1
    opobj['RequesterAnnotation'] = task_id
    # I think that this is all you need for the BOTO request
    # TODO: implement
    raise NotImplementedError()
