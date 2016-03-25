

from hirlite import Rlite
from collections import Counter

from htrc.corpus import Corpus


class CountData:


    def __init__(self, path):

        """
        Initialize the rlite connection.

        Args:
            path (str)
        """

        self.rlite = Rlite(path)


    def incr_token_count_for_year(self, year, token, count):

        """
        Increment the count for a token in a year by a given amount.

        Args:
            year (int)
            token (str)
            count (int)
        """

        self.rlite.command('hincrby', str(year), token, str(count))


    def token_count_for_year(self, year, token):

        """
        Get the total count for a token in a year.

        Args:
            year (int)
            token (str)
        """

        count = self.rlite.command('hmget', str(year), token)

        return int(count[0])


    # TODO|dev
    def index(self):

        """
        Index token counts.
        """

        corpus = Corpus.from_env()

        for volume in corpus.volumes():

            counts = Counter()

            for page in volume.pages():
                counts += page.total_counts()

            for token, count in counts.items():

                self.incr_token_count_for_year(
                    volume.year,
                    token,
                    count,
                )

            print(volume.id)
