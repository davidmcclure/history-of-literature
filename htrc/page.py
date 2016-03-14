

from itertools import combinations


class Page:


    def __init__(self, json):

        """
        Wrap an individual page.

        Args:
            json (dict)
        """

        self.json = json


    @property
    def token_counts(self):

        """
        Generate a set of flat (type, POS, count) 3-tuples.

        Yields: (token, POS, count)
        """

        for token, pc in self.json['body']['tokenPosCount'].items():
            for pos, count in pc.items():

                yield dict(
                    token=token,
                    pos=pos,
                    count=count,
                )


    @property
    def edges(self):

        """
        Generate a set of edge weight contributions.

        Yields: ((token1, POS), (token2, POS), count)
        """

        for tc1, tc2 in combinations(self.token_counts, 2):

            yield dict(

                token1=(tc1['token'], tc1['pos']),
                token2=(tc2['token'], tc2['pos']),

                # Take the smaller of the counts, to capture the number of
                # _pairs_ of the two words that appear together.
                count=min(tc1['count'], tc2['count'])

            )
