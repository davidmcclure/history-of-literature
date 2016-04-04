

from redis import StrictRedis


class Keyset:


    def __init__(self, db):

        """
        Initialize the Redis connection.

        Args:
            db (int)
        """

        self.redis = StrictRedis(db=db)
