

import os


def ensure_dir(path):

    """
    Create a directory, if it doesn't already exist.

    Args:
        path (str)
    """

    if not os.path.exists(path):
        os.makedirs(path)
