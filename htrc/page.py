

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
        Get a flat list of type,POS,count 3-tuples.

        Returns:
            list [(type, pos, count), ...]
        """

        counts = []

        for token, pc in self.json['body']['tokenPosCount'].items():
            for pos, count in pc.items():
                counts.append((token, pos, count))

        return counts
