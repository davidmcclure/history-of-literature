

import os
import scandir

from collections import Counter

from .volume import Volume


class Corpus:


    def __init__(self, path):

        """
        Canonicalize the corpus path.

        Args:
            path (str)
        """

        self.path = os.path.abspath(path)


    @property
    def volumes(self):

        """
        Generate volume instances.

        Yields: Volume
        """

        for root, dirs, files in scandir.walk(self.path):
            for name in files:

                path = os.path.join(root, name)
                yield Volume(path)


    @property
    def edges(self):

        """
        Assemble a complete edge list.

        Returns: EdgeList
        """

        edges = Counter()

        for volume in self.volumes:
            edges += volume.edges

        return edges
