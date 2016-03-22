

import os

from htrc.volume import Volume
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
    graph = vol.token_graph(token)
    graph.shelve(path)
