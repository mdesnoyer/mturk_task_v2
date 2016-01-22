"""
GLOBAL UTILITY FUNCTIONS
"""

"""
ID GENERATION
"""
_id_len = 16
def _rand_id_gen(n):
    """
    Generates random IDs
    :param n: The number of characters in the random ID
    :return: A raw ID string, composed of n upper- and lowercase letters as well as digits.
    """
    return ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(n))


def task_id_gen():
    """
    Generates task IDs
    :return: A task ID, as a string
    """
    return TASK_PREFIX + _rand_id_gen(_id_len)


def practice_id_gen():
    """
    Generates practice IDs
    :return: A practice ID, as a string
    """
    return PREFIX_PREFIX + _rand_id_gen(_id_len)