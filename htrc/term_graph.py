

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
