

import os

from multiprocessing import Pool
from itertools import repeat
from functools import partial

from htrc.corpus import Corpus
from htrc.token_graph import TokenGraph
from htrc import graph_jobs as jobs


class GraphData:


    def __init__(self, path):

        """
        Canonicalize the graph data path.

        Args:
            path (str)
        """

        self.path = os.path.abspath(path)


    @property
    def years_root(self):
        return os.path.join(self.path, 'years')


    @property
    def merged_root(self):
        return os.path.join(self.path, 'merged')


    def year_paths(self):

        """
        Generate year bucket paths.

        Yields: str
        """

        for entry in os.scandir(self.years_root):
            yield entry.path


    def write_volume_graphs(self, token, procs=8, logn=100):

        """
        Spawn graph writer procs.

        Args:
            token (str)
            procs (int)
        """

        corpus = Corpus.from_env()

        with Pool(procs) as pool:

            # Apply the token and base path.
            func = partial(
                jobs.write_volume_graph,
                token,
                self.years_root,
            )

            worker = pool.imap(func, corpus.paths())

            # Log progress.
            for i, _ in enumerate(worker):
                if i % logn == 0:
                    print(i)
