"""
Handles color logging.
"""

import logging
import logging.handlers
from colorlog import ColoredFormatter
from datetime import datetime


def _get_timestamp_string():
    return datetime.now().isoformat()


def config_root_logger(logfile=None):
    """
    Sets up the root logger. Call this in the main() of the file.

    :param logfile: The filename to log to.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(levelname)-8s %(asctime)s - %(name)s - %(funcName)s: %(message)s",
        datefmt='%m/%d/%Y %I:%M:%S %p')
    fltr = logging.Filter(name='mturk')
    if logfile is not None:
        # Add a rotating file handler
        handler = logging.handlers.RotatingFileHandler(
            logfile,
            maxBytes=104857600L,  # 100 MB
            backupCount=3)
        handler.addFilter(fltr)
        handler.setFormatter(formatter)
        logger.addHandler(handler)


def setup_logger(log_name):
    """
    Return a logger configured for a particular module.

    :param log_name: The name of the logger to use.
    """
    # TODO: Have this log to somewhere!
    formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s %(asctime)s - %(name)s - "
        "%(funcName)s: %(message)s",
        datefmt='%m/%d/%Y %I:%M:%S %p',
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red',
        }
    )
    # the mturk prefix is added so that it filters things that aren't mturk
    logger = logging.getLogger('mturk.' + log_name)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    if handler not in logger.handlers:
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger
