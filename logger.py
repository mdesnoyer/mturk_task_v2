"""
Handles color logging.
"""

import logging
import logging.handlers
from colorlog import ColoredFormatter
from datetime import datetime


def _get_timestamp_string():
    return datetime.now().isoformat()


def config_root_logger(logfile=None, return_webserver=False):
    """
    Sets up the root logger. Call this in the main() of the file.

    :param logfile: The filename to log to.
    :param return_webserver: Whether or not to also create and return a
           handler for the Flask webserver as well.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(levelname)-8s %(asctime)s - %(name)s - %(funcName)s: %(message)s",
        datefmt='%m/%d/%Y %I:%M:%S %p')
    fltr1 = logging.Filter(name='mturk')
    fltr2 = logging.Filter(name='apscheduler.scheduler')
    if logfile is not None:
        # Add a rotating file handler
        handler = logging.handlers.RotatingFileHandler(
            logfile,
            maxBytes=104857600L,  # 100 MB
            backupCount=6)
        handler.addFilter(fltr1)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        handler_aps = logging.handlers.RotatingFileHandler(
            logfile,
            maxBytes=104857600L,  # 100 MB
            backupCount=6)
        handler_aps.addFilter(fltr2)
        handler_aps.setFormatter(formatter)
        logger.addHandler(handler_aps)
        if return_webserver:
            handler = logging.handlers.RotatingFileHandler(
                logfile,
                maxBytes=104857600L,  # 100 MB
                backupCount=6)
            handler.setFormatter(formatter)
            handler.setLevel(logging.WARNING)
            return handler


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
