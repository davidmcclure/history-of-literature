#!/usr/bin/env python


import numpy as np

from mpi4py import MPI
from collections import defaultdict, Counter

from hol.corpus import Corpus
from hol.volume import Volume
from hol.models import Count


if __name__ == '__main__':

    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()

    # Scatter the path segments.

    if rank == 0:

        corpus = Corpus.from_env()

        paths = list(corpus.paths())

        data = np.array_split(paths, size)

    else:
        data = None

    data = comm.scatter(data, root=0)

    # Build up counts.

    page = defaultdict(Counter)

    for path in data:

        try:
            vol = Volume.from_path(path)
            page[vol.year] += vol.token_counts()
            print(path)

        except:
            pass

    # Gather counts, merge, flush to disk.

    pages = comm.gather(page, root=0)

    if rank == 0:

        result = defaultdict(Counter)

        for page in pages:
            for year, counts in page.items():
                result[year] += counts

        Count.flush_page(result)
