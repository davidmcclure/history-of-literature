

import os

from multiprocessing import Pool
from itertools import repeat

from htrc.corpus import Corpus
from htrc.volume import Volume
from htrc.token_graph import TokenGraph


def pool_write_volume_graph(data_path, token, procs=8):

    """
    Spawn graph writer procs.

    Args:
        data_path (str)
        token (str)
        procs (int)
    """

    corpus = Corpus.from_env()

    with Pool(procs) as pool:

        pool.starmap(write_volume_graph, zip(
            corpus.paths(),
            repeat(data_path),
            repeat(token),
        ))


def write_volume_graph(vol_path, data_path, token):

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
    ensure_dir(path)

    # Serialize the graph.
    graph = vol.token_graph(token)
    graph.shelve(path)

    print(path) # TODO|dev


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
