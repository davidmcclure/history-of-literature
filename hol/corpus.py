

import os
import scandir

from multiprocessing import Pool
from functools import partial

from hol import config
from hol.volume import Volume
from hol.utils import grouper



class Corpus:


    @classmethod
    def from_env(cls):

        """
        Wrap the ENV-defined corpus root

        Returns: cls
        """

        return cls(config['corpus'])


    def __init__(self, path):

        """
        Canonicalize the corpus path.

        Args:
            path (str)
        """

        self.path = os.path.abspath(path)


    def __len__(self):

        """
        How many volumes in the corpus?

        Returns: int
        """

        return len(list(self.paths))


    def paths(self):

        """
        Generate asset paths.

        Yields: str
        """

        for root, dirs, files in scandir.walk(self.path):
            for name in files:
                yield os.path.join(root, name)


    def path_groups(self, n=1000):

        """
        Generate groups of paths.

        Yields: list
        """

        for group in grouper(self.paths(), n):
            yield group


    def volumes(self):

        """
        Generate volume instances.

        Yields: Volume
        """

        for path in self.paths():
            yield Volume.from_path(path)


    def map(self, worker, num_procs=12, page_size=100):

        """
        Map a function across English volumes.

        Args:
            worker (func)
            num_procs (int)
            page_size (int)

        Yields: JobGroup
        """

        groups = self.path_groups(page_size)

        with Pool(num_procs) as pool:
            for paths in groups:

                jobs = pool.imap_unordered(
                    partial(read_vol, worker),
                    paths,
                )

                yield JobGroup(jobs)



def read_vol(worker, path):

    """
    Open a volume, apply a worker if it's English.

    Args:
        worker (func)
        path (str)

    Returns: mixed
    """

    try:

        vol = Volume.from_path(path)

        if vol.is_english:
            return worker(vol)

    except Exception as e:
        print(e)



class JobGroup:


    def __init__(self, jobs):

        """
        Wrap a job set.

        Args:
            jobs (IMapUnorderedIterator)
        """

        self.jobs = jobs


    def __iter__(self):

        """
        If the the worker was applied, yield the result.

        Yields: mixed
        """

        for result in self.jobs:
            if result:
                yield result
