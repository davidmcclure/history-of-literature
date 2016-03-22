

import os

from multiprocessing import Pool
from itertools import repeat

from htrc.corpus import Corpus
from htrc.volume import Volume


def pool_graphs(data_path, token, procs=8):

    """
    Spawn graph writer procs.

    Args:
        data_path (str)
        token (str)
        procs (int)
    """

    corpus = Corpus.from_env()

    with Pool(procs) as pool:

        pool.starmap(write_vol_graph, zip(
            corpus.paths(),
            repeat(data_path),
            repeat(token),
        ))


def write_vol_graph(vol_path, data_path, token):

    """
    Compute and serialize a volume graph.

    Args:
        vol_path (str)
        data_path (str)
        token (str)
    """

    vol = Volume(vol_path)

    # Form the serialization path.
    path = os.path.join(data_path, str(vol.year), vol.slug)
    dirname = os.path.dirname(path)

    # Ensure the directory.
    if not os.path.exists(dirname):
        os.makedirs(dirname)

    # Serialize the graph.
    graph = vol.token_graph(token)
    graph.shelve(path)

    print(path) # TODO|dev


def merge_year_graph(year, data_path):

    """
    Merge together the volume graphs for a given year.

    Args:
        year (int)
        data_path (str)
    """

    pass
