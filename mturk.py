"""
Exports a class that handles all MTurk side operations. This is the 'ying' to
db.py's 'yang'. The class handles, among other things:

    Add task awaiting HIT to HIT Group
    Check if uncompleted task is in HIT Group
    Ban worker (change status) (add temporary ban qualification)
    Unban worker (change status)
    Approve worker (change status) (add practice passed)
    Unapprove worker (change status)
    etc

    NOTES
        No function here modifies the database; operations that should take
        place on both MTurk and the database, like the creation of a task,
        should be made consistent with calls to both MTurk and db.Set by
        whatever is managing the instances of the two classes (see: daemon,
        webserver).

        Some confusion has been created (by me) when referring to HITs here.
        For the MTurk task, there is only one assignment per HIT. However,
        in the broader world of MTurk, this isn't always the case--some HITs
        can have multiple assignments--and hence HIT status and assignment
        status are not synonymous. (whoops!)
"""

from conf import *
import boto.mturk.connection
import boto.mturk.notification
import boto.mturk.qualification
import boto.mturk.price
import boto.mturk.question

_log = logger.setup_logger(__name__)

class _LocaleRequirement(boto.mturk.qualification.Requirement):
    """
    Similar to MTurk._create_qualification_type, this replicates boto.murk's
    functionality, but extends it to add some missing functionality. In this
    case, it adds the ability to specify multiple locales.
    """
    # TODO: Create a fork on github for this on the boto repo and fix it then
    #  issue a pull request!

    def __init__(self, comparator, locale, required_to_preview=False):
        super(_LocaleRequirement,
              self).__init__(qualification_type_id="00000000000000000071",
                             comparator=comparator, integer_value=None,
                             required_to_preview=required_to_preview)
        self.locale = locale

    def get_as_params(self):
        params =  {
            "QualificationTypeId": self.qualification_type_id,
            "Comparator": self.comparator,
        }
        if type(self.locale) is str:
            params['LocaleValue.Country'] = self.locale,
        else:
            for n, loc in enumerate(self.locale):
                params['LocaleValue.%i.Country' % (n + 1)] = loc
        if self.required_to_preview:
            params['RequiredToPreview'] = "true"
        return params


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
        self.mtconn = boto.mturk.connection.MTurkConnection(
            aws_access_key_id=MTURK_ACCESS_ID,
            aws_secret_access_key=MTURK_SECRET_KEY,
            host='mechanicalturk.sandbox.amazonaws.com')
        # TODO: delete the above!
        # TODO: figure out which functions can be discarded and stuff.
        self._get_qualification_ids()
        self._gen_requirement()
        self._gen_practice_requirement()
        self.current_balance = 0
        self.get_account_balance()  # get the current account balance
        _log.info('Current account funds: $%.2f' % self.current_balance)

    def _get_all_hits_of_type_by_status_selector(self, hit_type_id=None,
                                                 ids_only=False,
                                                 selector=lambda x: True):
        """
        Gets all hits according to a selector based on the HIT status.

        :param hit_type_id: The HIT type ID, as a string. If None, all hit
                            types  are searched.
        :param ids_only: If True, will return the IDs only.
        :param selector: A lambda function, which accepts a HIT status (see:
                         _globals.py) and returns a boolean.
        :return: A list of HIT objects. Or a list of HIT IDs, as strings.
        """
        iterator = self.mtconn.get_all_hits()
        hits = []
        for hit in iterator:
            if selector(self.get_hit_status(hit=hit)):
                if hit_type_id is None or hit.HITTypeId == hit_type_id:
                    if ids_only:
                        hits.append(hit.HITId)
                    else:
                        hits.append(hit)
        return hits

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
        qid = self._get_qualification_id_from_name(PRACTICE_QUOTA_NAME)
        if qid is None:
            self._gen_practice_quota_qualification()
        else:
            self.practice_quota_id = qid

    def _get_qualification_id_from_name(self, qualification_name):
        """
        Gets a specific qualification ID.

        :param qualification_name: The Qualification type name, as a string.
        :return: The ID of the qualification, as a string. Otherwise returns
                 None.
        """
        _log.info('Checking if qualification "%s" exists already' %
                  qualification_name)
        srch_resp = self.mtconn.search_qualification_types(
            query=qualification_name)
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
            resp = self.mtconn.create_qualification_type(
                name=QUALIFICATION_NAME,
                description=QUALIFICATION_DESCRIPTION, status='Active',
                is_requestable=False)
            self.qualification_id = resp[0].QualificationTypeId
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error creating main qualification: ' + e.message)

    def _gen_practice_quota_qualification(self):
        _log.info('Generating practice quota qualification')
        try:
            resp = self.mtconn.create_qualification_type(
                name=PRACTICE_QUOTA_NAME,
                description=PRACTICE_QUOTA_DESCRIPTION,
                status='Active',
                auto_granted=True,
                auto_granted_value=NUM_PRACTICES)
            self.practice_quota_id = resp[0].QualificationTypeId
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Failed creating practice quota qualification' +
                       e.message)

    def _gen_quota_qualification(self):
        """
        Creates the daily quota qualification, sets the ID internally.

        :return: None
        """
        _log.info('Generating quota qualification')
        try:
            resp = self.mtconn.create_qualification_type(
                name=DAILY_QUOTA_NAME, description=DAILY_QUOTA_DESCRIPTION,
                status='Active', is_requestable=False)
            self.quota_id = resp[0].QualificationTypeId
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error creating quota qualification: ' + e.message)

    def _gen_requirement(self):
        """
        Obtains a required qualification object for the 'true' task, and sets
        it internally.

        :return: None
        """
        requirements = \
            [boto.mturk.qualification.Requirement(self.qualification_id,
                                                  'Exists',
                                                  required_to_preview=True),
            boto.mturk.qualification.Requirement(self.quota_id,
                                                 'GreaterThan',
                                                 0,
                                                 required_to_preview=True),
            _LocaleRequirement('In', LOCALES)]
        self.qualification_requirement =  \
            boto.mturk.qualification.Qualifications(requirements=requirements)

    def _gen_practice_requirement(self):
        """
        Obtains a required qualification for doing the practice task,
        and sets it internally.

        :return: None
        """
        requirements = \
            [boto.mturk.qualification.Requirement(self.qualification_id,
                                                  'DoesNotExist'),
             _LocaleRequirement('In', LOCALES),
             boto.mturk.qualification.Requirement(self.practice_quota_id,
                                                  'GreaterThan',
                                                  0,
                                                  required_to_preview=True)]
        self.practice_qualification_requirement = \
            boto.mturk.qualification.Qualifications(requirements=requirements)

    def get_account_balance(self):
        """
        Checks the account balance. Also sets its value internally.

        :return: The account balance, in USD
        """
        resp = self.mtconn.get_account_balance()
        self.current_balance = resp[0].amount
        return self.current_balance

    def get_pending_hits(self):
        """
        Fetches the number of pending hits.

        :return: Number of hits that are in an 'assignable' state.
        """
        iterator = self.mtconn.get_all_hits()
        total_pending = 0
        for hit in iterator:
            if self.get_hit_status(hit=hit) == HIT_PENDING:
                total_pending += 1
        return total_pending

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

    def get_hit_status(self, hit_id=None, hit=None):
        """
        Gets the HIT status (see: _globals.py)

        :param hit_id: The HIT ID, as provided by MTurk, as a string.
        :param hit: The HIT object itself. If this is provided, hit_id is
                    unnecessary and it overrides HIT ID.
        :return: A HIT status.
        """
        if hit is None:
            if hit_id is None:
                raise ValueError('Must specify either hit_id or hit')
            try:
                hit = self.mtconn.get_hit(hit_id)[0]
            except IndexError:
                _log.error('Could not obtain HIT status for HIT %s' % hit_id)
                return HIT_UNDEFINED
        if hit.HITStatus == 'Disposed':
            return HIT_DISPOSED
        assignments = self.mtconn.get_assignments(hit.HITId)
        if len(assignments) == hit.MaxAssignments:
            if assignments[0].AssignmentStatus == 'Assignable':
                return HIT_APPROVED
            elif assignments[0].AssignmentStatus == 'Rejected':
                return HIT_REJECTED
            else:
                return HIT_COMPLETE
        if hit.HITStatus == 'Assignable':
            return HIT_PENDING
        elif hit.HITStatus == 'Unassignable':
            if hit.expired:
                return HIT_EXPIRED
            else:
                return HIT_DEAD

    def get_practice_status(self, hit_id=None, hit=None):
        """
        Gets the status of a practice HIT.

        :param hit_id: The HIT ID, as provided by MTurk, as a string.
        :param hit: The HIT object itself. If this is provided, hit_id is
        unnecessary and it overrides HIT ID.
        :return: A practice HIT status.
        """
        if hit is None:
            if hit_id is None:
                raise ValueError('Must specify either hit_id or hit')
            try:
                hit = self.mtconn.get_hit(hit_id)[0]
            except IndexError:
                _log.error('Could not obtain HIT status for practice HIT %s' %
                           hit_id)
                return PRACTICE_UNDEFINED
        if hit.expired:
            return PRACTICE_EXPIRED
        if hit.HITStatus == 'Disposed':
            return PRACTICE_DEAD
        if hit.HITStatus == 'Unassignable':
            assignments = self.mtconn.get_assignments(hit.HITId)
            if len(assignments) == hit.MaxAssignments:
                return PRACTICE_COMPLETE
            return PRACTICE_DEAD

    def get_hit(self, hit_id):
        """
        Get information about a HIT.

        :param hit_id: HIT ID, as a string, as supplied by MTurk.
        :return: A boto.mturk HIT object, else returns None.
        """
        try:
            hit_info = self.mtconn.get_hit(hit_id)[0]
            return hit_info
        except boto.mturk.connection.MTurkRequestError as e:
            _log.warn('Getting HIT information failed with: %s' % e.message)
            return None

    def get_all_hits_of_type(self, hit_type_id=None, ids_only=False):
        """
        Gets all hits of a certain type. If no hit_type_id is specified,
        then it returns hit IDs from across all hit types.

        :param hit_type_id: The HIT type ID, as a string. If None, all hit
        types are searched.
        :param ids_only: If True, will return the IDs only.
        :return: A list of HIT objects. Or a list of HIT IDs, as strings.
        """
        return self._get_all_hits_of_type_by_status_selector(
            hit_type_id=hit_type_id,
            ids_only=ids_only,
            selector=lambda x: True)

    def get_all_pending_hits_of_type(self, hit_type_id=None, ids_only=False):
        """
        Returns all pending HITs of a specified hit_type_id. If no
        hit_type_id is specified, then it returns hits  from across all hit
        types.

        :param hit_type_id: The HIT type ID, as a string. If None, all hit
                            types are searched.
        :param ids_only: If True, will return the IDs only.
        :return: A list of HIT objects. Or a list of HIT IDs, as strings.
        """
        return self._get_all_hits_of_type_by_status_selector(
            hit_type_id=hit_type_id,
            ids_only=ids_only,
            selector=lambda x: x == HIT_PENDING)

    def get_all_incomplete_hits_of_type(self, hit_type_id=None, ids_only=False):
        """
        Returns all incomplete hits of a type.

        :param hit_type_id: The HIT type ID, as a string. If None, all hit
                            types are searched.
        :param ids_only: If True, will return the IDs only.
        :return: A list of HIT objects. Or a list of HIT IDs, as strings.
        """
        return self._get_all_hits_of_type_by_status_selector(
            hit_type_id=hit_type_id,
            ids_only=ids_only,
            selector=lambda x: x == HIT_PENDING or x == HIT_EXPIRED)

    def get_all_processed_hits_of_type(self, hit_type_id=None, ids_only=False):
        """
        Returns all processed (i.e., approved / rejected) hits of a type.

        :param hit_type_id: The HIT type ID, as a string. If None, all hit
                            types are searched.
        :param ids_only: If True, will return the IDs only.
        :return: A list of HIT objects. Or a list of HIT IDs, as strings.
        """
        return self._get_all_hits_of_type_by_status_selector(
            hit_type_id=hit_type_id,
            ids_only=ids_only,
            selector=lambda x: x == HIT_APPROVED or x == HIT_REJECTED)

    def grant_worker_practice_passed(self, worker_id):
        """
        Grants worker the qualification necessary to begin attempting to
        complete our real tasks.

        NOTE:
            This does NOT update the database to reflect the fact that the
            worker has become qualified!

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        try:
            self.mtconn.assign_qualification(self.qualification_id, worker_id)
            # be sure to grant them a daily quota
            self.reset_worker_daily_quota(worker_id)
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error granting worker main qualification: %s' +
                       e.message)

    def revoke_worker_practice_passed(self, worker_id, reason=None):
        """
        Revokes the qualification necessary to begin attempting to complete
        our real tasks from a worker.

        :param worker_id: The MTurk worker ID.
        :param reason: Why the practice was revoked.
        :return: None
        """
        try:
            # NOTE: They refer to 'worker_id' idiosyncratically here as
            # "subject_id"...
            self.mtconn.revoke_qualification(worker_id,
                                             self.qualification_id,
                                             reason=reason)
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error revoking worker practice passed qualification: '
                       '%s' + e.message)

    def reset_worker_daily_quota(self, worker_id):
        """
        Resets a worker's daily quota, allowing them to complete another
        round of tasks, as set by MAX_SUBMITS_PER_DAY (see conf.py)

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        try:
            self.mtconn.assign_qualification(self.quota_id, worker_id,
                                             value=MAX_SUBMITS_PER_DAY,
                                             send_notification=False)
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error resetting daily quota for worker: %s' + e.message)

    def reset_worker_weekly_practice_quota(self, worker_id):
        """
        Resets a worker's weekly practice quota, allowing them to complete
        another round of practices, as set by NUM_PRACTICES (see conf.py)

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        try:
            self.mtconn.assign_qualification(self.practice_quota_id, worker_id,
                                             value=NUM_PRACTICES,
                                             send_notification=False)
        except boto.mturk.connection.MTurkRequestError as e:
            _log.error('Error resetting weekly practice quota for worker: %s' +
                       e.message)

    def decrement_worker_daily_quota(self, worker_id):
        """
        Decrements the worker's daily quota for submittable tasks by one.

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        try:
            quota_val = \
                self.mtconn.get_qualification_score(self.quota_id, worker_id)
            if quota_val > 0:
                quota_val -= 1
            else:
                _log.warn('Worker %s quota already at or below zero.' %
                          worker_id)
        except boto.mturk.connection.MTurkRequestError:
            _log.warn('Could not obtain quota for worker %s' % worker_id)
            quota_val = 0
        self.mtconn.assign_qualification(self.quota_id, worker_id,
                                         value=quota_val,
                                         send_notification=False)

    def decrement_worker_practice_weekly_quota(self, worker_id):
        """
        Decrements the worker's weekly practice quota by one.

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        try:
            quota_val = \
                self.mtconn.get_qualification_score(self.practice_quota_id,
                                                    worker_id)
            if quota_val > 0:
                quota_val -= 1
            else:
                _log.warn('Worker %s quota already at or below zero.' %
                          worker_id)
        except boto.mturk.connection.MTurkRequestError:
            _log.warn('Could not obtain quota for worker %s' % worker_id)
            quota_val = 0
        self.mtconn.assign_qualification(self.practice_quota_id, worker_id,
                                         value=quota_val,
                                         send_notification=False)

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

        :param worker_id: The MTurk worker ID.
        :return: None
        """
        self.mtconn.unblock_worker(worker_id,
                                   reason='Your ban has expired, you are free '
                                          'to attempt our tasks again.')

    def approve_assignment(self, assignment_id):
        """
        Approves an assignment.

        :param assignment_id: The ID of the assignment in question as
                              provided by MTurk.
        :return: None.
        """
        self.mtconn.approve_assignment(assignment_id)

    def reject_assignment(self,  assignment_id, reason=None):
        """
        Rejects a HIT.

        :param assignment_id: The ID of the assignment in question as
                              provided by MTurk.
        :param reason: The reason the HIT was rejected.
        :return: None.
        """
        self.mtconn.reject_assignment(assignment_id, feedback=reason)

    def soft_reject_assignment(self, assignment_id, reason=None):
        """
        Soft rejects a hit: i.e., approves a hit but provides feedback.

        :param assignment_id: The assignment ID.
        :param reason: The warning to provide to the worker. This may be a list.
        :return: None
        """
        feedback = 'While we are accepting assignment %s we found the ' \
                   'following problem(s):\n\n'
        if type(reason) is str:
            reason = [reason]
        for specific_reason in reason:
            feedback += specific_reason + '\n'
        feedback += '\n'
        feedback += 'If we continue to find problems in your HITs, you will ' \
                    'be temporarily banned.'
        self.mtconn.approve_assignment(assignment_id, feedback=feedback)

    def register_hit_type_mturk(self,
                                title=DEFAULT_TASK_NAME,
                                practice_title=DEFAULT_PRACTICE_TASK_NAME,
                                description=DESCRIPTION,
                                practice_description=PRACTICE_DESCRIPTION,
                                reward=DEFAULT_TASK_PAYMENT,
                                practice_reward=DEFAULT_PRACTICE_PAYMENT,
                                hit_type_duration=HIT_TYPE_DURATION,
                                keywords=KEYWORDS,
                                auto_approve_delay=AUTO_APPROVE_DELAY):
        """
        Registers a new HIT type.

        NOTES:

            ** THIS FUNCTION DOES NOT MODIFY THE DATABASE **

            The idea of practices and 'true' tasks is abstracted away here.
            It is assumed that for every HIT type, there will be defined
            practice and true tasks. Hence:

                ! THIS FUNCTION CREATES BOTH A PRACTICE AND A TRUE HIT TYPE
                ID ON MTURK!

            Because it does not modify the database, you do not provide it
            with a task attribute or image attributes.

        :param title: The HIT type title.
        :param practice_title: The HIT type title for practices.
        :param description: The HIT Type description.
        :param practice_description: The HIT type description to accompany
                                     practice tasks.
        :param reward: The reward for completing this type of HIT.
        :param practice_reward: The reward for completing practices of this
                                HIT type.
        :param hit_type_duration: How long this HIT type persists for.
        :param keywords: The HIT type keywords.
        :param auto_approve_delay: The auto-approve delay.
        :return: The HIT type IDs, in order ('True' HIT Type, Practice HIT
                 Type). Raises an error if not successful.
        """
        true_opobj = dict()
        prac_opobj = dict()
        # define the operation object for the 'true' task HIT type ID
        true_opobj['title'] = title
        true_opobj['description'] = description
        true_opobj['reward'] = reward
        true_opobj['duration'] = hit_type_duration
        true_opobj['keywords'] = keywords
        true_opobj['approval_delay'] = auto_approve_delay
        true_opobj['qual_req'] = self.qualification_requirement
        resp = self.mtconn.register_hit_type(**true_opobj)
        true_hit_type_id = resp[0].HITTypeId
        # define the operation object for the practice task HIT type ID
        prac_opobj['title'] = practice_title
        prac_opobj['description'] = practice_description
        prac_opobj['reward'] = practice_reward
        prac_opobj['duration'] = hit_type_duration
        prac_opobj['keywords'] = keywords
        prac_opobj['approval_delay'] = auto_approve_delay
        prac_opobj['qual_req'] = self.practice_qualification_requirement
        resp = self.mtconn.register_hit_type(**prac_opobj)
        practice_hit_type_id = resp[0].HITTypeId
        return true_hit_type_id, practice_hit_type_id

    def add_hit_to_hit_type(self, hit_type_id, task_id):
        """
        Creates a mechanical turk task, and assigns it to the HIT type
        specified. This function exposes internally-generated tasks to MTurk.

        NOTES:

            ** THIS FUNCTION DOES NOT MODIFY THE DATABASE **

        :param hit_type_id: A string, specifying the HIT type using its ID.
        :param task_id: The task ID that this HIT will serve up.
        :return: None if successful, else raises an error.
        """
        question_object = \
            boto.mturk.question.ExternalQuestion(
                external_url=EXTERNAL_QUESTION_ENDPOINT,
                frame_height=BOX_SIZE[1]+200)
        # TODO: Make sure the frame_height is correct.
        opobj = dict()
        opobj['hit_type'] = hit_type_id
        opobj['question'] = question_object
        opobj['lifetime'] = HIT_LIFETIME_IN_SECONDS
        opobj['max_assignments'] = 1
        opobj['annotation'] = task_id
        resp = self.mtconn.create_hit(**opobj)
        return resp[0].HITId

    def add_practice_hit_to_hit_type(self, hit_type_id, task_id):
        """
        Creates a mechanical turk task that points to a practice trial,
        and assigns it to the HIT type specified. This function exposes
        internally-generated tasks to MTurk.

        NOTES:
            ** THIS FUNCTION DOES NOT MODIFY THE DATABASE **

        :param hit_type_id: A string, specifying the HIT type using its ID.
        :param task_id: The task ID that this HIT will serve up.
        :return: None if successful, else raises an error.
        """
        question_object = \
            boto.mturk.question.ExternalQuestion(
                external_url=EXTERNAL_QUESTION_ENDPOINT,
                frame_height=BOX_SIZE[1]+200)
        # TODO: Make sure the frame_height is correct.
        opobj = dict()
        opobj['hit_type'] = hit_type_id
        opobj['question'] = question_object
        opobj['lifetime'] = PRACTICE_TASK_LIFETIME
        opobj['max_assignments'] = NUM_ASSIGNMENTS_PER_PRACTICE
        opobj['annotation'] = task_id
        resp = self.mtconn.create_hit(**opobj)
        return resp[0].HITId

    def dispose_of_hit_type(self, hit_type_id=None):
        """
        Disposes of a hit type.

        :param hit_type_id: The HIT type ID, as a string, as provided by MTurk
        :return: None
        """
        if hit_type_id is None:
            _log.warn('Not Implemented: MTurk API currently does not support '
                      'querying all hit types!')
            return
        self.dispose_hit(hit_type_id)

    def disable_all_hits_of_type(self, hit_type_id=None):
        """
        Disposes all hits of a certain hit type. If hit_type_id is undefined,
        it disposes ALL hits that have been posted. All submitted tasks are
        automatically approved. HITs that cannot be

        :param hit_type_id: The hit type ID, as a string. If None, all hit
                            types are searched.
        :return: None.
        """
        hits = self.get_all_hits_of_type(hit_type_id=hit_type_id, ids_only=True)
        fail_disabled = []
        disabled = 0
        fail_disposed = []
        disposed = 0
        for hit_id in hits:
            if not self.disable_hit(hit_id):
                fail_disabled.append(hit_id)
            else:
                disabled += 1
        for hit_id in fail_disabled:
            if not self.dispose_hit(hit_id):
                fail_disposed.append(hit_id)
            else:
                disposed += 1
        _log.info('Disabled %i HITs, disposed of %i HITs, %i still exist.' %
                  (disabled, disposed, len(fail_disposed)))

    def disable_handled_hits(self):
        """
        Disables all HITs where all the assignments have been completed and
        have been approved / rejected

        :return: None
        """
        _log.info('Searching for completed hits')
        hits = self.get_all_hits_of_type()
        tot = 0
        dis = 0
        for hit in hits:
            tot += 1
            if hit.HITStatus == 'Disposed':
                continue
            assignments = self.mtconn.get_assignments(hit.HITId)
            if len(assignments) == hit.MaxAssignments:
                disable_able = True
                for ass in assignments:
                    if ass.AssignmentStatus == 'Approved':
                        continue
                    if ass.AssignmentStatus == 'Rejected':
                        continue
                    disable_able = False
                    break
                if disable_able:
                    dis += 1
                    self.disable_hit(hit.HITId)
        _log.info('%i HITs found, %i disabled' % (tot, dis))

    def extend_all_hits_of_type(self, hit_type_id=None,
                                extension_amount=DEF_EXTENSION_TIME):
        """
        Extends all hits of a specified type by the extension_amount,
        which is specified in seconds. If no hit_type_id is specified,
        then it applies to all hits across all types.

        :param hit_type_id: The HIT type ID, as a string. If None, all hit
                            types are searched.
        :param extension_amount: The length of time to extend it by in seconds.
        :return: None.
        """
        hits = self.get_all_incomplete_hits_of_type(hit_type_id=hit_type_id,
                                                    ids_only=True)
        for hit in hits:
            self.extend_hit(hit, extension_amount)

    def disable_hit(self, hit_id):
        """
        Disables a HIT.

        :param hit_id: The HIT ID, as provided by MTurk, as a string.
        :return: True if successful, False otherwise.
        """
        try:
            self.mtconn.disable_hit(hit_id)
            _log.debug('Disabled hit %s' % hit_id)
            return True
        except boto.mturk.connection.MTurkRequestError as e:
            _log.debug('Could not disable hit %s: %s' % (hit_id, e.message))
            return False

    def dispose_hit(self, hit_id):
        """
        Disposes of a hit.

        :param hit_id: The HIT ID, as provided by MTurk, as a string.
        :return: True if successful, False otherwise.
        """
        try:
            self.mtconn.dispose_hit(hit_id)
            _log.debug('Disposed of hit %s' % hit_id)
            return True
        except boto.mturk.connection.MTurkRequestError as e:
            _log.debug('Could not dispose of hit %s: %s' % (hit_id, e.message))
            return False

    def extend_hit(self, hit_id, extension_amount=DEF_EXTENSION_TIME):
        """
        Extends a HIT by extension_amount.

        :param hit_id: The HIT ID, as a string.
        :param extension_amount: The length of time to extend it by in seconds.
        :return: None.
        """
        _log.info('Extending hit %s by %.0f seconds' % (hit_id,
                                                        extension_amount))
        self.mtconn.extend_hit(hit_id, expiration_increment=extension_amount)
