

from mpi4py import MPI

from hol.corpus import Corpus


class Job:


    @staticmethod
    def process_paths(self, paths):
        raise NotImplementedError


    def flush_result(self, result):
        raise NotImplementedError


    def __init__(self, group_size):

        """
        Set the path group size.

        Args:
            group_size (int)
        """

        self.group_size = group_size


    def run(self):

        """
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
                    self.flush_result(data)

                # EXIT
                elif tag == 3:
                    closed += 1

        else:

            while True:

                # Notify ready.
                comm.send(None, dest=0, tag=1)

                # Request paths.
                paths = comm.recv(
                    source=0,
                    tag=MPI.ANY_SOURCE,
                    status=status,
                )

                tag = status.Get_tag()

                # Extract counts.
                if tag == 1:
                    result = self.process_paths(paths)
                    comm.send(result, dest=0, tag=2)

                # Or, no paths, exit.
                elif tag == 3:
                    break

            comm.send(None, dest=0, tag=3)
