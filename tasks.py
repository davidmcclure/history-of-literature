

import os

from invoke import task
from multiprocessing import Pool
from itertools import repeat
from datetime import datetime as dt

from htrc.corpus import Corpus
from htrc.volume import Volume


@task
def test_pool():

    """
    Test multiprocessing.
    """

    corpus = Corpus('data/basic')

    with Pool(processes=8) as pool:

        pool.starmap(
            write_graph,
            zip(corpus.paths(), repeat('literature'))
        )


def write_graph(vol_path, token):

    """
    Compute and store a community graph.

    Args:
        vol_path (str)
        token (str)
    """

    vol = Volume(vol_path)

    path = 'graphs/{0}/{1}'.format(vol.year, vol.slug)
    dirname = os.path.dirname(path)

    # Ensure directory.
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # Serialize the graph.
    graph = vol.token_graph(token)
    graph.shelve(path)

    print(path)
