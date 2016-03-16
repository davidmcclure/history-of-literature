

import json
import bz2

from functools import reduce
from collections import Counter

from htrc.page import Page
from htrc.term_graph import TermGraph


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


    def pages(self):

        """
        Generate page instances.

        Yields: Page
        """

        for json in self.json['features']['pages']:
            yield Page(json)


    def token_graph(self, token, *args, **kwargs):

        """
        Assemble a co-occurrence graph for pages that contain a token.

        Args:
            token (str)

        Returns: TermGraph
        """

        graph = TermGraph()

        for page in self.pages():
            if page.has_token(token):
                graph += page.graph(*args, **kwargs)

        return graph
