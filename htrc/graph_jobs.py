

import os

from htrc.volume import Volume
from htrc.token_graph import TokenGraph
from htrc.utils import ensure_dir


def write_volume_graph(token, out_path, vol_path):

    """
    Compute and serialize a volume graph.

    Args:
        token (str)
        out_path (str)
        vol_path (str)
    """

    vol = Volume(vol_path)

    # Form the serialization path.
    path = os.path.join(out_path, str(vol.year), vol.slug)
    ensure_dir(path)

    # Serialize the graph.
    graph = vol.spoke_graph(token)
    graph.shelve(path)


def merge_year_graph(out_path, year_path):

    """
    Compute and serialize a volume graph.

    Args:
        out_path (str)
        year_path (str)
    """

    # Get the year path part.
    year = os.path.basename(year_path)

    # Form the serialization path.
    path = os.path.join(out_path, year)
    ensure_dir(path)

    for entry in os.scandir(year_path):

        # Merge into the year graph.
        graph = TokenGraph.from_shelf(entry.path)
        graph.shelve(path)

    print(year)
