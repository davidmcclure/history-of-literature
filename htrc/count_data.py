

import os

from multiprocessing import Pool
from functools import partial

from htrc.corpus import Corpus
from htrc.volume import Volume


def merge_volume_counts(path):

    """
    Extract filtered token counts from a volume.

    Args:
        path (str): A HTRC volume path.

    Returns: dict
    """

    vol = Volume(path)

    return vol.total_counts()
