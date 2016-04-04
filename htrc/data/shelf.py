

import shelve


class Shelf:


    def __init__(self, path):

        """
        Open the shelf.

        Args:
            path (str)
        """

        self.data = shelve.open(path)
