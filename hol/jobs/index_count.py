

import numpy as np
import click

from collections import defaultdict, Counter
from mpi4py import MPI

from hol.corpus import Corpus
from hol.volume import Volume
from hol.models import Count


# @click.command()
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

        path_groups = corpus.path_groups(10)

        closed = 0
        while closed < size-1:

            # Get a work request from a slot.
            data = comm.recv(
                status=status,
                source=MPI.ANY_SOURCE,
                tag=MPI.ANY_TAG,
            )

            source = status.Get_source()
            tag = status.Get_tag()

            # READY
            if tag == 1:

                # Get a path group.
                try:
                    paths = next(path_groups)

                # If finished, close the worker.
                except StopIteration:
                    comm.send(None, dest=source, tag=3)

                # Otherwise, send the paths.
                comm.send(list(paths), dest=source, tag=1)

            # RESULT
            elif tag == 2:
                Count.flush(data)

            # EXIT
            elif tag == 3:
                closed += 1

    else:

        while True:

            comm.send(None, dest=0, tag=1)

            paths = comm.recv(
                source=0,
                tag=MPI.ANY_SOURCE,
                status=status,
            )

            tag = status.Get_tag()

            if tag == 1:
                counts = extract_counts(paths)
                comm.send(counts, dest=0, tag=2)

            elif tag == 3:
                break

        comm.send(None, dest=0, tag=3)


def extract_counts(paths):

    """
    Accumulate counts for a set of paths.

    Args:
        paths (list)

    Returns: defaultdict(Counter)
    """

    counts = defaultdict(Counter)

    for path in paths:

        try:

            vol = Volume.from_path(path)

            if vol.is_english:
                counts[vol.year] += vol.token_counts()

            print(path)

        except Exception as e:
            print(e)

    return counts


if __name__ == '__main__':
    index_count()
