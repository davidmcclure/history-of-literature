

import os
import scandir

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


    def graph(self, *args, **kwargs):

        """
        Assemble the co-occurrence graph for the entire corpus.

        Returns: TermGraph
        """

        graph = TermGraph()

        volumes = progress.bar(
            self.volumes(),
            expected_size=len(list(self.paths()))
        )

        for volume in volumes:
            graph += volume.graph(*args, **kwargs)

        return graph
