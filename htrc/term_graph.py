

import networkx as nx
import shelve


class TermGraph(nx.Graph):


    @classmethod
    def from_shelf(cls, path):

        """
        Hydrate an instance from a shelf file.

        Args:
            path (str)

        Returns: cls
        """

        graph = cls()

        with shelve.open(path) as data:
            for key, count in data.items():

                # Split the tokens.
                t1, t2 = key.split(':')

                # Register the edge.
                graph.add_edge(t1, t2, weight=count)

        return graph


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


    def shelve(self, path):

        """
        Index edges into a shelf file.
        """

        with shelve.open(path) as data:
            for t1, t2, count in self.edge_index_iter():

                key = '{0}:{1}'.format(t1, t2)

                if key in data:
                    data[key] += count

                else:
                    data[key] = count
