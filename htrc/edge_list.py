

class EdgeList(dict):


    def __iadd__(self, other):

        """
        Merge in another set of weights.

        Args:
            other (EdgeList)

        Returns: EdgeList
        """

        for (token1, token2), count in other.items():
            self.add_weight(token1, token2, count)

        return self


    def add_weight(self, token1, token2, count):

        """
        Register a weight contribution for a pair of tokens.

        Args:
            token1 (str)
            token2 (str)
            count (int)
        """

        pair = (token1, token2)

        # Bump the count, if the pair has been seen.
        if pair in self:
            self[pair] += count

        # Or, initialize the value.
        else:
            self[pair] = count
