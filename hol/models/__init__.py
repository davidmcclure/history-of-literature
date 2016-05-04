

import numpy as np

from collections import defaultdict, Counter
from mpi4py import MPI

from hol.corpus import Corpus
from hol.volume import Volume

from .base import Base
from .anchored_count import AnchoredCount
from .count import Count
from .score import Score


def index_count():

    """
    Index raw token counts.
    """

    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()

    # Scatter path segments.

    data = None

    if rank == 0:

        corpus = Corpus.from_env()

        data = np.array_split(list(corpus.paths()), size)

    paths = comm.scatter(data, root=0)

    # Tabulate the token counts.

    page = defaultdict(Counter)

    for path in paths:

        try:

            vol = Volume.from_path(path)

            if vol.is_english:
                page[vol.year] += vol.token_counts()

        except Exception as e:
            print(e)

    # Gather the results, merge, flush to disk.

    pages = comm.gather(page, root=0)

    if rank == 0:

        merged = defaultdict(Counter)

        for page in pages:
            for year, counts in page.items():
                merged[year] += counts

        Count.flush_page(merged)


def index_anchored_count(anchor, size=1000):

    """
    Index anchored token counts.

    Args:
        anchor (str)
    """

    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()

    # Scatter path segments.

    data = None

    if rank == 0:

        corpus = Corpus.from_env()

        data = np.array_split(list(corpus.paths()), size)

    paths = comm.scatter(data, root=0)

    # Tabulate the token counts.

    page = defaultdict(lambda: defaultdict(Counter))

    for path in paths:

        try:

            vol = Volume.from_path(path)

            if vol.is_english:

                level_counts = vol.anchored_token_counts(anchor, size)

                for level, counts in level_counts.items():
                    page[vol.year][level] += counts

        except Exception as e:
            print(e)

    # Gather the results, merge, flush to disk.

    pages = comm.gather(dict(page), root=0)

    if rank == 0:

        merged = defaultdict(lambda: defaultdict(Counter))

        for page in pages:
            for year, level_counts in page.items():
                for level, counts in level_counts.items():
                    merged[year][level] += counts

        AnchoredCount.flush_page(merged)
