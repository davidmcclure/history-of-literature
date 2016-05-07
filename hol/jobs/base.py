

from datetime import datetime as dt
from abc import ABCMeta, abstractmethod
from mpi4py import MPI

from hol.corpus import Corpus
from hol.utils import enum


Tags = enum('READY', 'WORK', 'RESULT', 'EXIT')


class BaseJob(metaclass=ABCMeta):


    @abstractmethod
    def process(self, paths):
        pass


    @abstractmethod
    def shrinkwrap(self, paths):
        pass


    @abstractmethod
    def merge(self, result):
        pass


    @abstractmethod
    def flush(self):
        pass


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

        i = 0
        if rank == 0:

            t1 = dt.now()

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

                    # Try to send a new path group.
                    try:
                        paths = next(path_groups)
                        comm.send(list(paths), dest=source, tag=Tags.WORK)
                        print(source, 'work')

                    # If finished, close the worker.
                    except StopIteration:
                        comm.send(None, dest=source, tag=Tags.EXIT)
                        print(source, 'exit')

                # RESULT
                elif tag == Tags.RESULT:

                    # Log progress.
                    print((i+1) * self.group_size, 'merge')
                    i += 1

                # EXIT
                elif tag == Tags.EXIT:
                    self.merge(data)
                    closed += 1

            self.flush()

            # Log total duration.
            t2 = dt.now()
            print(t2-t1)

        else:

            while True:

                # Notify ready.
                comm.send(None, dest=0, tag=Tags.READY)
                print(rank, 'ready')

                # Request paths.
                paths = comm.recv(
                    source=0,
                    tag=MPI.ANY_SOURCE,
                    status=status,
                )

                tag = status.Get_tag()

                # Extract counts.
                if tag == Tags.WORK:
                    self.process(paths)
                    comm.send(None, dest=0, tag=Tags.RESULT)
                    print(rank, 'result')

                # Or, no paths, exit.
                elif tag == Tags.EXIT:
                    break

            data = self.shrinkwrap()

            # Send data back to 0.
            comm.send(data, dest=0, tag=Tags.EXIT)
