

import networkx as nx


class TermGraph(nx.Graph):


    def __iadd__(self, other):

        """
        Merge in edges / weights from another graph.

        Args:
            other (nx.Graph)

        Returns: nx.Graph
        """

        for t1, t2, data in other.edges_iter(data=True):

            weight = data.get('weight')

            # If the edge exists, bump the weight.

            if self.has_edge(t1, t2):
                self[t1][t2]['weight'] += weight

            # Otherwise, initialize the edge.

            else:
                self.add_edge(t1, t2, weight=weight)

        return self


    def edge_index_iter(self):

        """
        Generate normalized pair/count pairs for the edge index.

        Yields: (t1, t2, count)
        """

        for t1, t2, data in self.edges_iter(data=True):

            # Ensure consistent ordering.
            t1, t2 = sorted([t1, t2])

            yield (t1, t2, data['weight'])


    def invert_weights(self):

        """
        Make edge weights reflect "cost" or "distance" - the more frequently
        two words co-occur, the lower the weight.
        """

        weights = [
            d['weight'] for t1, t2, d in
            self.edges_iter(data=True)
        ]

        return weights
