

import os

from collections import OrderedDict
from cached_property import cached_property

from htrc.token_graph import TokenGraph
from htrc.utils import sort_dict_by_key


class YearGraphs:


    def __init__(self, path):

        """
        Canonicalize the root path.

        Args:
            path (str)
        """

        self.path = os.path.abspath(path)


    def year_paths(self):

        """
        Generate tuples that link a year with a graph path.

        Yields: (year<int>, path<str>)
        """

        paths = OrderedDict()

        for entry in os.scandir(self.path):
            year = os.path.basename(entry.path)
            paths[int(year)] = entry.path

        return sort_dict_by_key(paths)


    def graph_by_year(self, year):

        """
        Hydrate the graph for a year.

        Args:
            year (int)

        Returns: TokenGraph
        """

        path = os.path.join(self.path, str(year))

        return TokenGraph.from_shelf(path)


    def all_tokens(self):

        """
        Get a set of all tokens in the graphs.

        Returns: set
        """

        tokens = set()

        for entry in os.scandir(self.path):

            graph = TokenGraph.from_shelf(entry.path)
            tokens.update(graph.nodes())

        return tokens
