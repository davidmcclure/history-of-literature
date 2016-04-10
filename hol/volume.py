

import json
import bz2

from collections import Counter, defaultdict

from hol.page import Page


class Volume:


    def __init__(self, path):

        """
        Read the compressed volume archive.

        Args:
            path (str)
        """

        with bz2.open(path, 'rt') as fh:
            self.json = json.loads(fh.read())


    @property
    def id(self):

        """
        Get the HTRC id.

        Returns: str
        """

        return self.json['id']


    @property
    def slug(self):

        """
        Get a filesystem-friendly version of the id.

        Returns: str
        """

        return self.id.replace('/', '-')


    @property
    def year(self):

        """
        Get the publication year.

        Returns: int
        """

        return int(self.json['metadata']['pubDate'])


    @property
    def language(self):

        """
        Get the language.

        Returns: str
        """

        return self.json['metadata']['language']


    @property
    def token_count(self):

        """
        Get the total number of tokens in the page "body" sections.

        Returns: int
        """

        total = 0

        for page in self.pages():
            total += page.token_count

        return total


    def pages(self):

        """
        Generate page instances.

        Yields: Page
        """

        for json in self.json['features']['pages']:
            yield Page(json)


    def cleaned_token_counts(self):

        """
        Count the total count of each token in all pages.

        Returns: Counter
        """

        counts = Counter()

        for page in self.pages():
            counts += page.cleaned_token_counts()

        return counts
