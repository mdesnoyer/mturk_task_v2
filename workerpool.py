"""
Exports a class, ThreadPool, that executes arbitrary functions on MTurk and
HBase. This is done in a threaded manner, and is useful for enqueueing tasks
that just need to modify the database and don't actually need to return
anything (i.e., generating new tasks, approving stuff, etc). I'm not doing this
using mutliprocessing.ThreadPool because we do not need to duplicate the
MTurk and the HBase objects in memory; mostly, the issue arises from the
delays in communication. I want the Workers to have access to the objects
without having to copy them. Additionally, it appears that ThreadPool is
meant to apply stuff in batches, rather than randomly adding them, as far as
I know.

Additionally, exports Scheduler, which permits tasks to be run on a schedule.

Important info:
    Each worker gets its own mturk, dbget, dbset instances. Thus,
    all functions to which it is assigned must accept these as its first
    three functions, in that order, as the first arguments.

Adapapted from:
http://stackoverflow.com/a/7257510
"""

from Queue import Queue
from threading import Thread
from threading import Event
from db import Get
from db import Set
from mturk import MTurk
import boto
import happybase
from conf import *
import traceback

_log = logger.setup_logger(__name__)

if MTURK_SANDBOX:
    mturk_host = MTURK_SANDBOX_HOST
else:
    mturk_host = MTURK_HOST


class _Worker(Thread):
    """
    Worker objects actually perform the execution of the functions passed to
    them with the arguments.
    """
    def __init__(self, tasks, mtconn, dbconn):
        """
        :param tasks: Tasks is a Queue which stores tasks. These processes
                      run as daemons, so you do not need to worry about
                      terminating them.
        :param mtconn: The MTurk connection object.
        :param dbconn: The database connection object.
        :return: A Worker() instance.
        """
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.mt = MTurk(mtconn)
        self.dbget = Get(dbconn)
        self.dbset = Set(dbconn)
        self.start()

    def run(self):
        """
        Dequeues an task from the Tasks queue, and executes the function.

        :return: None
        """
        while True:
            try:
                queue_obj = self.tasks.get()
                func, args, kwargs = queue_obj
                func(self.mt, self.dbget, self.dbset, *args, **kwargs)
            except Exception as e:
                traceback.print_exc()
            finally:
                self.tasks.task_done()


class ThreadPool:
    """
    Implements a thread pool, which manages workers who are evaluating tasks
    which return no functions.
    """
    def __init__(self, num_threads):
        """
        :param num_threads: The number of workers to run.
        :return: None.
        """
        self.tasks = Queue(num_threads)
        for _ in range(num_threads):
            mtconn = boto.mturk.connection.MTurkConnection(
                        aws_access_key_id=MTURK_ACCESS_ID,
                        aws_secret_access_key=MTURK_SECRET_KEY,
                        host=mturk_host)
            dbconn = happybase.Connection(host=DATABASE_LOCATION)
            _Worker(self.tasks, mtconn, dbconn)

    def add_task(self, func, *args, **kwargs):
        """
        Adds a task to the queue.

        :param func: The function to evaluate.
        :param args: Function arguments.
        :param kwargs: Function keyword arguments.
        :return: None.
        """
        self.tasks.put((func, args, kwargs))

    def wait_completion(self):
        """
        Waits for all the jobs to be finished.

        :return: None.
        """
        self.tasks.join()


class _ScheduledTask(Thread):
    """
    Represents the scheduled tasks, which run as a separate thread.
    """
    def __init__(self, event, interval, task_list, arg_list, kwarg_list,
                 mtconn, dbconn):
        """
        Returns a scheduled task object.

        :param event: A threading event, used internally by Scheduler to halt a
                      task.
        :param interval: The interval over which to run the task.
        :param task_list: A list of tasks (i.e., functions) to call. Similar
                         to how ThreadPool works, these must accept mt,
                         dbget, and dbset as their first arguments.
        :param arg_list: A list of arguments to provide each item in
                         task_list, in the same order.
        :param kwarg_list: A list of keyword arguments to provide each item in
                           task_list, in the same order.
        :param mtconn: The MTurk connection object.
        :param dbconn: The database connection object.
        :return: A _ScheduledTask object.
        """
        Thread.__init__(self)
        self.daemon = True  # ensure it's a daemon
        self.stopped = event
        self.interval = interval
        self.mt = MTurk(mtconn)
        self.dbget = Get(dbconn)
        self.dbset = Set(dbconn)
        self.task_list = task_list
        self.arg_list = arg_list
        self.kwarg_list = kwarg_list

    def run(self):
        """
        Starts the _ScheduledTask running.

        :return: None
        """
        while not self.stopped.wait(self.interval):
            for task, args, kwargs in zip(self.task_list,
                                          self.arg_list,
                                          self.kwarg_list):
                try:
                    task(self.mt, self.dbget, self.dbset, *args, **kwargs)
                except Exception as e:
                    traceback.print_exc()
        _log.info('Terminating')


class Scheduler:
    """
    Implements tasks that can run on a schedule.
    """
    def __init__(self, interval):
        """
        Instantiates a Scheduler object.

        :param interval: The interval over which to run the task.
        :return: A Scheduler object.
        """
        self.interval = interval
        self.stopped = Event()
        self.started = False
        self.task_list = []
        self.arg_list = []
        self.kwarg_list = []

    def add_task(self, func, *args, **kwargs):
        """
        Adds a task to the scheduled list of tasks.

        :param func: The function to evaluate.
        :param args: Function arguments.
        :param kwargs: Function keyword arguments.
        :return: None.
        """
        if self.started:
            _log.error('Cannot add task after schedule has been started!')
            return
        self.task_list.append(func)
        self.arg_list.append(args)
        self.kwarg_list.append(kwargs)

    def start(self):
        """
        Starts an instance of _ScheduledTask

        :return: None
        """
        _log.info('Starting scheduler')
        mtconn = boto.mturk.connection.MTurkConnection(
                        aws_access_key_id=MTURK_ACCESS_ID,
                        aws_secret_access_key=MTURK_SECRET_KEY,
                        host=mturk_host)
        dbconn = happybase.Connection(host=DATABASE_LOCATION)
        thread = _ScheduledTask(self.stopped,
                                self.interval,
                                self.task_list,
                                self.arg_list,
                                self.kwarg_list,
                                mtconn,
                                dbconn)
        thread.start()
        self.started = True

    def stop(self):
        """
        Halts the instance of _ScheduleTask

        :return: None
        """
        self.stopped.set()
        self.started = False














