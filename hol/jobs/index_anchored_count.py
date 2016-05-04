

import numpy as np
import click

from collections import defaultdict, Counter
from mpi4py import MPI

from hol.corpus import Corpus
from hol.volume import Volume
from hol.models import AnchoredCount


@click.command()

@click.argument('anchor')

@click.option(
    '--page_size',
    help='Group pages together into N-token chunks.',
)

def index_anchored_count(anchor, page_size=1000):

    """
    Index anchored token counts.

    Args:
        anchor (str)
        page_size (int)
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

        AnchoredCount.flush(merged)


if __name__ == '__main__':
    index_anchored_count()
