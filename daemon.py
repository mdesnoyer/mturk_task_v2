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
"""

# TODO: determine if it's possible to set the daemon's database access priority lower than the webservers.

from conf import *

_log = logger.setup_logger(__name__)



