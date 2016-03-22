

import os


def ensure_dir(path):

    """
    Ensure that a file path's directory exists.

    Args:
        path (str)
    """

    os.makedirs(
        os.path.dirname(path),
        exist_ok=True,
    )
