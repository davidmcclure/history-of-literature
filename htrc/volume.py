

import json
import bz2

from functools import reduce

from .page import Page
from .edge_list import EdgeList


class Volume:


    def __init__(self, path):

        """
        Read the compressed volume archive.

        Args:
            path (str)
        """

        fh = bz2.open(path, 'rt')

        self.json = json.loads(fh.read())


    @property
    def id(self):

        """
        Get the HTRC id.

        Returns: str
        """

        return self.json['id']


    @property
    def year(self):

        """
        Get the publication year.

        Returns: int
        """

        return int(self.json['metadata']['pubDate'])


    @property
    def pages(self):

        """
        Generate page instances.

        Yields: Page
        """

        for json in self.json['features']['pages']:
            yield Page(json)


    @property
    def edges(self):

        """
        Assemble combined edge weights for all pages.

        Returns:
            dict { (token1, token2): count }
        """

        edges = EdgeList()

        for page in self.pages:
            edges += page.edges

        return edges
