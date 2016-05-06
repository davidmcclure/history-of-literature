

from mpi4py import MPI

from hol.corpus import Corpus
from hol.utils import enum


Tags = enum('READY', 'WORK', 'RESULT', 'EXIT')


class Job:


    def process_paths(self, paths):
        raise NotImplementedError


    def flush_result(self, result):
        raise NotImplementedError


    def __init__(self, group_size=1000):

        """
        Set the path group size.

        Args:
            group_size (int)
        """

        self.group_size = group_size


    def run(self):

        """
        Pipe all paths through `process_paths` and flush the results.

        - Generate path groups from the corpus.
        - Dispatch each group to a MPI rank.
        - The rank processes the paths, sends back a result object.
        - The controller process flushes the result.
        """

        comm = MPI.COMM_WORLD

        size = comm.Get_size()
        rank = comm.Get_rank()

        status = MPI.Status()

        if rank == 0:

            corpus = Corpus.from_env()

            path_groups = corpus.path_groups(self.group_size)

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
                if tag == Tags.READY:

                    # Get a path group.
                    try:
                        paths = next(path_groups)

                    # If finished, close the worker.
                    except StopIteration:
                        comm.send(None, dest=source, tag=Tags.EXIT)
                        print('exit', source)

                    # Otherwise, send the paths.
                    comm.send(list(paths), dest=source, tag=Tags.WORK)
                    print('work', source)

                # RESULT
                elif tag == Tags.RESULT:
                    self.flush_result(data)

                # EXIT
                elif tag == Tags.EXIT:
                    closed += 1

        else:

            while True:

                # Notify ready.
                comm.send(None, dest=0, tag=Tags.READY)

                # Request paths.
                paths = comm.recv(
                    source=0,
                    tag=MPI.ANY_SOURCE,
                    status=status,
                )

                tag = status.Get_tag()

                # Extract counts.
                if tag == Tags.WORK:
                    result = self.process_paths(paths)
                    comm.send(result, dest=0, tag=Tags.RESULT)

                # Or, no paths, exit.
                elif tag == Tags.EXIT:
                    break

            comm.send(None, dest=0, tag=3)
