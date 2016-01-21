"""
This script exports functions for dealing with the types of MTurk transactions.
"""

from conf import *
import boto


"""
FUNCTIONS WE WILL NEED:

(1) Add task awaiting HIT to HIT Group
(2) Check if uncompleted task is in HIT Group
(3) Ban worker (add temporary ban qualification)
(4) Unban worker.
(5) Approve worker (add practice passed)
(6) Unapprove worker.

"""