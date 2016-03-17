

import os
import scandir
import shelve

from clint.textui import progress

from htrc.volume import Volume
from htrc.term_graph import TermGraph


class Corpus:


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


    def volumes(self):

        """
        Generate volume instances.

        Yields: Volume
        """

        for path in self.paths():
            yield Volume(path)


    def shelve_edges(self, path, token, *args, **kwargs):

        """
        Index edges via shelve.

        Args:
            token (str): The anchor token.
            path (str): The data file path.
        """

        volumes = progress.bar(
            self.volumes(),
            expected_size=len(list(self.paths()))
        )

        with shelve.open(path) as data:
            for volume in volumes:
                volume.shelve_edges(data, token, *args, **kwargs)
