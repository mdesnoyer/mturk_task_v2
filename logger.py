"""
Handles color logging.
"""

import logging
from colorlog import ColoredFormatter

def setup_logger(log_name):
    """Return a logger with a default ColoredFormatter."""
    formatter = ColoredFormatter(
        "%(log_color)s%(levelname)-8s [%(funcName)s] %(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG':    'cyan',
            'INFO':     'green',
            'WARNING':  'yellow',
            'ERROR':    'red',
            'CRITICAL': 'red',
        }
    )

    logger = logging.getLogger(log_name)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    # github says that you can only add a handler once:
    #   http://stackoverflow.com/questions/6333916/python-logging-ensure-a-handler-is-added-only-once
    # this does not appear to be the case, and so I'll perform the check here.
    if handler not in logger.handlers:
        logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    return logger