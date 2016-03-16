

import json
import bz2

from functools import reduce
from collections import Counter

from htrc.page import Page
from htrc.term_graph import TermGraph
from htrc.models import Session, Edge


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


    def graph(self, *args, **kwargs):

        """
        Assemble the co-occurrence graph for all pages.

        Returns: TermGraph
        """

        graph = TermGraph()

        for page in self.pages():
            graph += page.graph(*args, **kwargs)

        return graph


    def index_edges(self):

        """
        Index edges into the database.
        """

        graph = self.graph()

        session = Session()
        for t1, t2, data in graph.edges_iter(data=True):

            weight = data.get('weight')

            edge = Edge(
                token1=t1,
                token2=t2,
                year=self.year,
                weight=weight,
            )

            session.add(edge)

        session.commit()
