

import os

from invoke import task
from multiprocessing import Pool
from itertools import repeat

from htrc.corpus import Corpus
from htrc.volume import Volume


@task
def index_graphs(data_path, token, procs=8):

    """
    Index graphs by year.

    Args:
        data_path (str)
        token (str)
    """

    corpus = Corpus.from_env()

    with Pool(procs) as pool:

        pool.starmap(write_graph, zip(
            corpus.paths(),
            repeat(data_path),
            repeat(token),
        ))


def write_graph(vol_path, data_path, token):

    """
    Compute and store a community graph.

    Args:
        vol_path (str)
        token (str)
    """

    vol = Volume(vol_path)

    # Form the graph path.
    path = os.path.join(data_path, str(vol.year), vol.slug)
    dirname = os.path.dirname(path)

    # Ensure directory.
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # Serialize the graph.
    graph = vol.token_graph(token)
    graph.shelve(path)

    print(path)
