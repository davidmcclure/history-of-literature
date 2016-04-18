

import json
import bz2

from collections import Counter, defaultdict

from hol.page import Page


class Volume:


    @classmethod
    def from_path(cls, path):

        """
        Inflate a volume and make an instance.

        Args:
            path (str)

        Returns: cls
        """

        with bz2.open(path, 'rt') as fh:
            return cls(json.loads(fh.read()))


    def __init__(self, data):

        """
        Read the compressed volume archive.

        Args:
            data (dict)
        """

        self.data = data


    @property
    def id(self):

        """
        Get the HTRC id.

        Returns: str
        """

        return self.data['id']


    @property
    def year(self):

        """
        Get the publication year.

        Returns: int
        """

        return int(self.data['metadata']['pubDate'])


    @property
    def language(self):

        """
        Get the language.

        Returns: str
        """

        return self.data['metadata']['language']


    @property
    def is_english(self):

        """
        Is the volume English?

        Returns: bool
        """

        return self.language == 'eng'


    def pages(self):

        """
        Generate page instances.

        Yields: Page
        """

        for data in self.data['features']['pages']:
            yield Page(data)


    def token_counts(self):

        """
        Count the total count of each token in all pages.

        Returns: Counter
        """

        counts = Counter()

        for page in self.pages():
            counts += page.token_counts()

        return counts


    def anchored_token_counts(self, anchor):

        """
        Get counts for tokens that appear on pages with an "anchor" token,
        broken out by the count of thethe anchor on the page.

        Returns: dict
        """

        counts = defaultdict(Counter)

        for page in self.pages():

            page_counts = page.token_counts()

            level = page_counts.pop(anchor, None)

            if level:
                counts[level] += page_counts

        return counts
