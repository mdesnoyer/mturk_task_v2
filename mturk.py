"""
Exports a class that handles all MTurk side operations. This is the 'ying' to db.py's 'yang'. The class handles, among
other things:

    Add task awaiting HIT to HIT Group
    Check if uncompleted task is in HIT Group
    Ban worker (change status) (add temporary ban qualification)
    Unban worker (change status)
    Approve worker (change status) (add practice passed)
    Unapprove worker (change status)

    NOTES
        No function here modifies the database; operations that should take place on both MTurk and the database, like
        the creation of a task, should be made consistent with calls to both MTurk and db.Set by whatever is managing
        the instances of the two classes (see: daemon, webserver).
"""

from conf import *
import boto.mturk.connection
import boto.mturk.notification
import boto.mturk.qualification
import boto.mturk.price
import boto.mturk.question

_log = logger.setup_logger(__name__)

# !!!!!!!!!!!!!!!!!!!!!!!!!!! this is only here temporarily! (for autocompletion in pycharm)
# same deal with the stuff in MTurk's __init__ function.
# TODO: remove this!
MTURK_ACCESS_ID = os.environ['MTURK_ACCESS_ID']
MTURK_SECRET_KEY = os.environ['MTURK_SECRET_KEY']
# !!!!!!!!!!!!!!!!!!!!!!!!!!! end temporary stuff


class MTurk(object):
    """
    Handles all MTurk operations.
    """
    def __init__(self, mtconn):
        """
        :param mtconn: The Boto mechanical turk connection object.
        :return: An instance of MTurk.
        """
        self.mtconn = mtconn
        # TODO: delete the below!
        self.mtconn = boto.mturk.connection.MTurkConnection(aws_access_key_id=MTURK_ACCESS_ID,
                                                            aws_secret_access_key=MTURK_SECRET_KEY,
                                                            host='mechanicalturk.sandbox.amazonaws.com')
        # TODO: delete the above!
        self._get_qualification_ids()  # fetch the qualification names
        self._gen_requirement()  # fetch the qualification requirement for the 'true' task
        self._gen_practice_requirement()  # fetch the qualification for the practice task
        # TODO: Print out number of active tasks, current funds, etc.
        # TODO: Send warning if the funds get too low?

    def _get_qualification_ids(self):
        """
        Gets the qualification IDs, and sets them internally.

        :return: None
        """
        qid = self._get_qualification_id_from_name(QUALIFICATION_NAME)
        if qid is None:
            # you need to generate the qualification
            self._gen_main_qualification()
        else:
            self.qualification_id = qid
        qid = self._get_qualification_id_from_name(DAILY_QUOTA_NAME)
        if qid is None:
            self._gen_quota_qualification()
        else:
            self.quota_id = qid

    def _get_qualification_id_from_name(self, qualification_name):
        """
        Gets a specific qualification ID.

        :param qualification_name: The Qualification type name, as a string.
        :return: The ID of the qualification, as a string. Otherwise returns None.
        """
        _log.info('Checking if qualification %s exists already' % qualification_name)
        srch_resp = self.mtconn.search_qualification_types(query=qualification_name)
        for qual in srch_resp:
            if qual.Name == qualification_name:
                return qual.QualificationTypeId
        return None

    def _gen_main_qualification(self):
        """
        Creates the 'practiced passed' qualification, sets the ID internally.

        :return: None
        """
        _log.info('Generating main qualification')
        try:
            resp = self.mtconn.create_qualification_type(name=QUALIFICATION_NAME,
                                                         description=QUALIFICATION_DESCRIPTION,
                                                         status='Active')
            self.qualification_id = resp[0].QualificationTypeId
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error creating main qualification: ' + e.message)

    def _gen_quota_qualification(self):
        """
        Creates the daily quota qualification, sets the ID internally.

        :return: None
        """
        _log.info('Generating quota qualification')
        try:
            resp = self.mtconn.create_qualification_type(name=DAILY_QUOTA_NAME,
                                                         description=DAILY_QUOTA_DESCRIPTION,
                                                         status='Active')
            self.qualification_id = resp[0].QualificationTypeId
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error creating quota qualification: ' + e.message)

    def _gen_requirement(self):
        """
        Obtains a required qualification object for the 'true' task, and sets it internally.

        :return: None
        """
        requirements = [boto.mturk.qualification.Requirement(self.qualification_id, 'Exists',
                                                             required_to_preview=True),
                        boto.mturk.qualification.Requirement(self.quota_id, 'GreaterThan', 0,
                                                             required_to_preview=True)]
        self.qualification_requirement = boto.mturk.qualification.Qualifications(requirements=requirements)

    def _gen_practice_requirement(self):
        """
        Obtains a required qualification for doing the practice task, and sets it internally.

        :return: None
        """
        requirements = [boto.mturk.qualification.Requirement(self.qualification_id, 'DoesNotExist')]
        self.practice_qualification_requirement = boto.mturk.qualification.Qualifications(requirements=requirements)

    def grant_worker_practice_passed(self, worker_id):
        """
        Grants worker the qualification necessary to begin attempting to complete our real tasks.

        NOTE:
            This does NOT update the database to reflect the fact that the worker has become qualified!

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        try:
            self.mtconn.assign_qualification(self.qualification_id, worker_id)
            self.reset_worker_daily_quota(worker_id)  # be sure to grant them a daily quota
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error granting worker main qualification: %s' + e.message)

    def reset_worker_daily_quota(self, worker_id):
        """
        Resets a worker's daily quota, allowing them to complete another round of tasks, as set by MAX_SUBMITS_PER_DAY
        (see conf.py)

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        try:
            self.mtconn.assign_qualification(self.quota_id, worker_id, value=MAX_SUBMITS_PER_DAY,
                                             send_notification=False)
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error resetting daily quota for worker: %s' + e.message)

    def decrement_worker_daily_quota(self, worker_id):
        """
        Decrements the worker's daily quota for submittable tasks by one.

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        try:
            quota_val = self.mtconn.get_qualification_score(self.quota_id, worker_id)
            if quota_val > 0:
                quota_val -= 1
            else:
                _log.warn('Worker %s quota already at or below zero.' % worker_id)
        except boto.mturk.connection.MTurkRequestError:
            _log.warn('Could not obtain quota for worker %s' % worker_id)
            quota_val = 0
        self.mtconn.assign_qualification(self.quota_id, worker_id, value=quota_val, send_notification=False)

    def revoke_worker_practice_passed(self, worker_id, reason=None):
        """
        Revokes the qualification necessary to begin attempting to complete our real tasks from a worker.

        :param worker_id: The MTurk worker ID.
        :param reason: Why the practice was revoked.
        :return: None
        """
        try:
            # NOTE: They refer to 'worker_id' idiosyncratically here as "subject_id"...
            self.mtconn.revoke_qualification(worker_id, self.qualification_id, reason=reason)
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error revoking worker practice passed qualification: %s' + e.message)

    def ban_worker(self, worker_id, reason=DEFAULT_BAN_REASON):
        """
        Bans a worker. Also revokes their qualification.

        :param worker_id: The MTurk worker ID.
        :param reason: The reason for the ban.
        :return: None
        """
        # revoke the workers qualifications
        self.revoke_worker_practice_passed(worker_id, reason=reason)
        self.mtconn.block_worker(worker_id, reason=reason)

    def unban_worker(self, worker_id):
        """
        Un-bans a worker.

        :param mtconn: The Boto mechanical turk connection object.
        :param worker_id: The MTurk worker ID.
        :return: None
        """
        self.mtconn.unblock_worker(worker_id, reason='Your ban has expired, you are free to attempt our tasks '
                                                     'once more.')

    def get_hit_complete(self, hit_id=None):
        """
        Determines if a task has been completed or not.

        :param hit_id: The ID of the hit in question as provided by MTurk.
        :return: True if the task/HIT has been completed, otherwise false.
        """
        try:
            resp = self.mtconn.get_hit(hit_id)
            return resp[0].HITStatus in ['Reviewable', 'Reviewing', 'Disposed']
        except:
            return False

    def approve_assignment(self, assignment_id):
        """
        Approves an assignment.

        :param assignment_id: The ID of the assignment in question as provided by MTurk.
        :return: None.
        """
        self.mtconn.approve_assignment(assignment_id)

    def reject_hit(self,  assignment_id, reason=None):
        """
        Rejects a HIT.

        :param assignment_id: The ID of the assignment in question as provided by MTurk.
        :param reason: The reason the HIT was rejected.
        :return: None.
        """
        self.mtconn.reject_assignment(assignment_id, feedback=reason)

    def register_hit_type_mturk(self, title=DEFAULT_TASK_NAME, practice_title=DEFAULT_PRACTICE_TASK_NAME,
                                description=DESCRIPTION, practice_description=PRACTICE_DESCRIPTION,
                                reward=DEFAULT_TASK_PAYMENT, practice_reward=DEFAULT_PRACTICE_PAYMENT,
                                assignment_duration=ASSIGNMENT_DURATION, keywords=KEYWORDS,
                                auto_approve_delay=AUTO_APPROVE_DELAY):
        """
        Registers a new HIT type.

        NOTES:

            ** THIS FUNCTION DOES NOT MODIFY THE DATABASE **

            The idea of practices and 'true' tasks is abstracted away here. It is assumed that for every HIT type, there
            will be defined practice and true tasks. Hence:

                ! THIS FUNCTION CREATES BOTH A PRACTICE AND A TRUE HIT TYPE ID ON MTURK!

            Because it does not modify the database, you do not provide it with a task attribute or image attributes.

        :param title: The HIT type title.
        :param practice_title: The HIT type title for practices.
        :param description: The HIT Type description.
        :param practice_description: The HIT type description to accompany practice tasks.
        :param reward: The reward for completing this type of HIT.
        :param practice_reward: The reward for completing practices of this HIT type.
        :param assignment_duration: How long this HIT type persists for.
        :param keywords: The HIT type keywords.
        :param auto_approve_delay: The auto-approve delay.
        :return: The HIT type IDs, in order ('True' HIT Type, Practice HIT Type). Raises an error if not successful.
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
        true_opobj['qual_req'] = self.qualification_requirement
        resp = self.mtconn.register_hit_type(**true_opobj)
        true_hit_type_id = resp[0].HITTypeId
        # define the operation object for the practice task HIT type ID
        prac_opobj['title'] = practice_title
        prac_opobj['description'] = practice_description
        prac_opobj['reward'] = practice_reward
        prac_opobj['duration'] = assignment_duration
        prac_opobj['keywords'] = keywords
        prac_opobj['approval_delay'] = auto_approve_delay
        prac_opobj['qual_req'] = self.practice_qualification_requirement
        resp = self.mtconn.register_hit_type(**prac_opobj)
        practice_hit_type_id = resp[0].HITTypeId
        return true_hit_type_id, practice_hit_type_id

    def add_hit_to_hit_type(self, hit_type_id, task_id, is_practice=False, reward=None):
        """
        Creates a mechanical turk task, and assigns it to the HIT type specified.

        NOTES:

            ** THIS FUNCTION DOES NOT MODIFY THE DATABASE **

        :param conn: The HappyBase connection object.
        :param mtconn: The Boto mechanical turk connection object.
        :param hit_type_id: A string, specifying the HIT type using its ID.
        :param task_id: The task ID that this HIT will serve up.
        :param is_practice: Boolean, indicating whether or not this task is a practice.
        :param reward: The reward amount (as a float).
        :return: None if successful, else raises an error.
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
        opobj['max_assignments'] = 1
        opobj['annotation'] = task_id
        resp = self.mtconn.create_hit(**opobj)
        return resp[0].HITId
