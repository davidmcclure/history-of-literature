

from hol import config


class QuerySet:


    def __init__(self):

        """
        Initialize a session.
        """

        self.session = config.Session()
