

import json
import bz2

from functools import reduce


class Volume:


    def __init__(self, path):

        """
        Read the compressed volume archive.

        Args:
            path (str)
        """

        fh = bz2.open(path, 'rt')

        self.json = json.loads(fh.read())


    def get(self, *keys):

        """
        Get a nested path in the dict.

        Args
            *keys (str): A list of nested keys.

        Returns: mixed|None
        """

        return reduce(dict.get, keys, self.json)


    @property
    def id(self):

        """
        Get the HTRC id.

        Returns: str
        """

        return self.get('id')


    @property
    def year(self):

        """
        Get the publication year.

        Returns: int
        """

        return int(self.get('metadata', 'pubDate'))
