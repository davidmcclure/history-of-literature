

import json
import bz2

from functools import reduce

from .page import Page


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

        edges = {}

        for page in self.pages:
            for pair, count in page.edges:

                # Bump the count, if the pair has been seen.
                if pair in edges:
                    edges[pair] += count

                # Or, initialize the value.
                else:
                    edges[pair] = count

        return edges
