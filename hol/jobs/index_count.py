

import numpy as np
import click

from collections import defaultdict, Counter
from mpi4py import MPI

from hol.corpus import Corpus
from hol.volume import Volume
from hol.models import Count


@click.command()
def index_count():

    """
    Index year -> token -> count.
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

            print(path)

        except Exception as e:
            print(e)

    # Gather the results, merge, flush to disk.

    pages = comm.gather(page, root=0)

    if rank == 0:

        merged = defaultdict(Counter)

        for page in pages:
            for year, counts in page.items():
                merged[year] += counts

        print('flush')

        Count.flush(merged)


if __name__ == '__main__':
    index_count()