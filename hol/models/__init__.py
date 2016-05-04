

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

    counts = defaultdict(Counter)

    for path in paths:

        try:

            vol = Volume.from_path(path)

            if vol.is_english:
                counts[vol.year] += vol.token_counts()

            print(path)

        except Exception as e:
            print(e)

    # Gather the results, merge, flush to disk.

    results = comm.gather(counts, root=0)

    merged = defaultdict(Counter)

    for result in results:
        for year, counts in result.items():
            merged[year] += counts

    Count.flush_page(merged)
