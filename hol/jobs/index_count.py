

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

    status = MPI.Status()

    if rank == 0:

        corpus = Corpus.from_env()

        for paths in corpus.path_groups(10):

            data = comm.recv(
                status=status,
                source=MPI.ANY_SOURCE,
                tag=MPI.ANY_TAG,
            )

            source = status.Get_source()
            tag = status.Get_tag()

            # READY
            if tag == 1:
                comm.send(list(paths), dest=source)

            # DONE
            elif tag == 2:
                print(data)

    else:
        while True:

            # Request a path segment.
            comm.send(None, dest=0, tag=1)
            paths = comm.recv(source=0)

            print(paths)

            # Extract the counts.
            page = defaultdict(Counter)
            for path in paths:

                try:

                    vol = Volume.from_path(path)

                    if vol.is_english:
                        page[vol.year] += vol.token_counts()

                    print(path)

                except Exception as e:
                    print(e)

            # Return the counts.
            comm.send(page, dest=0, tag=2)


if __name__ == '__main__':
    index_count()
