

import json
import bz2

from collections import Counter, defaultdict

from hol.page import Page
from hol.utils import group_counts


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


    def anchored_token_counts(self, anchor, size=1000):

        """
        Get counts for tokens that appear on (grouped) pages with an "anchor"
        token, broken out by the count of thethe anchor on the page.

        Args:
            anchor (str)
            size (int)

        Returns: dict
        """

        pages = list(self.pages())

        counts = [p.total_token_count for p in pages]

        groups = group_counts(counts, size)

        levels = defaultdict(Counter)

        i = 0
        for group in groups:

            chunk = Counter()

            for _ in group:
                chunk += pages[i].token_counts()
                i += 1

            level = chunk.pop(anchor, None)

            if level:
                levels[level] += chunk

        return levels
