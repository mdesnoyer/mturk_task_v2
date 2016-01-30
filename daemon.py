"""
Exports a class, the daemon, that is designed to run in parallel alongside the webserver. Essentially, this class
corrals db.get, db.set, and mturk so that the data and activity among them are up to date and appropriate, both for
our needs and for the needs of our workers.

This amounts to:

    - Banning workers
    - Unbanning workers
    - Approving / rejecting tasks in the database
    - Approving / rejecting tasks on MTurk
    - Renewing tasks that have expired
    - Building new practices
    - Activating new images.

This should be done continuously, as such the daemon simply runs between those tasks, iteratively. Importantly, some
tasks that may seem as though they should be done "offline" or in parallel with the webserver should be done
on-demand, to keep workers happy, such as granting the practice passed qualification.

In sequence, the daemon:
    - Checks for tasks that need validation (pseudo on-demand)
        - Post-validation, check if this merit a bans.
    - Check banned workers if it's time to unban them.
    - Checks if new tasks are needed
    - Checks if the practices need to be refreshed
    - Checks if workers need to be unbanned

NOTES:
    The Daemon also uses the database db connection and mturk connection objects, and thus should not be run in
    parallel with the webserver in the true sense. Instead, it should be spawned as a separate thread, which share
    memory in python because of the GIL.

    To perform the "pseudo on-demand" validation of tasks, the Daemon also accepts a queue, from which it dequeues
    task IDs to validate.
"""

# TODO: determine if it's possible to set the daemon's database access priority lower than the webservers.

from conf import *
from itertools import cycle
import Queue

_log = logger.setup_logger(__name__)

class Daemon():
    """
    This class is designed to run as a daemon thread, which continuously manages MTurk and the database to do all the
    housekeeping, issuing of new HITs, etc (see readme above). Any work that does not need to be done on-demand as a
    result of web requests hitting the server.
    """
    def __init__(self, dbget, dbset, mt, work_queue):
        """
        Instantiates the Daemon class.

        :param dbget: An instance of the database 'getter' class. (see db.py)
        :param dbset: An instance of the database 'setter' class. (see db.py)
        :param mt: An instance of the MTurk class. (see mturk.py)
        :param work_queue: A multithreading Queue object, which stores all the 'important' jobs.
        :return: Instance object.
        """
        self.dbget = dbget
        self.dbset = dbset
        self.mt = mt
        self.q = work_queue
        # the 'loop functions' are a sequence of functions to run, one after the other, each time loop() is called.
        self.loop_functions = cycle([self.check_images, self.check_practices, self.check_images, self.check_unban])
        # the termination attribute. If True, the thread exits. (set by "loop")
        self.terminate = False
        self.setup()

    def setup(self):
        """
        This should be run once the Daemon is spun up. It ensures the following:
            - There are enough images activated.
            - There is an active HIT Type ID for normal tasks and practice tasks.
            - There are enough HITs posted.
            - There are enough practices posted, which are <= 1 week old.

        :return: None
        """
        # TODO: Implement this!
        raise NotImplementedError()

    def loop(self):
        """
        This is the main function of the Daemon; it defines a loop that runs all the required tasks, and between each
        checks to ensure that the queue is empty.

        :return: None
        """
        # check if you should terminate
        if self.terminate:
            return
        try:
            # attempt to fetch a task_id from the queue
            task_id = self.q.get(False)
            if task_id is None:
                _log.info('Terminating!')
                self.terminate = True
                return
            else:
                _log.info('Validating task %s' % task_id)
                self.validate_work(task_id)
        except Queue.Empty:
            self.loop_functions.next()

    def check_hits(self):
        """
        Checks that there are enough HITs posted.

        :return: None
        """
        _log.info('Checking all hits on MTurk')
        # TODO: Implement this!
        pass

    def check_practices(self):
        """
        Checks that there are enough practices posted, and that they're not too old.

        :return: None
        """
        _log.info('Checking all practice tasks on MTurk')
        # TODO: Implement this!
        pass

    def check_ban(self):
        """
        Checks to see if any users need to be banned.

        NOTES:
            Not currently used (implicit in the validate_work call)

        :return: None
        """
        pass

    def check_unban(self):
        """
        Checks to see if any banned workers need to be unbanned.

        :return: None
        """
        _log.info('Checking if banned workers can be unbanned')
        # TODO: Implement this!
        pass

    def reset_quotas(self):
        """
        Resets all worker quotas.

        :return: None
        """
        _log.info('Resetting all quotas')
        # TODO: Implement this!
        pass

    def check_images(self):
        """
        Checks to see if more images need to be activated.

        :return: None
        """
        _log.info('Checking all images')
        # TODO: Implement this!
        pass

    def validate_work(self, task_id=None):
        """
        Invokes the validation script on a task.

        :return: None
        """
        is_acceptable = self.dbget.task_is_acceptable(task_id)
        if is_acceptable:
            self.dbset.accept_task(task_id)
        else:
            self.dbset.reject_task(task_id)
