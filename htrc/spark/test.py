

from pyspark import SparkContext, SparkConf

from htrc.corpus import Corpus
from htrc.volume import Volume


conf = SparkConf().setAppName('test').setMaster('local')
sc = SparkContext(conf=conf)


# Wrap path collection.
paths = Corpus('data/basic').paths()
paths_dist = sc.parallelize(paths)


def count_tokens(path):
    v = Volume(path)
    return v.token_count


def make_graph(path):
    v = Volume(path)
    return v.graph(min_freq=1e-04)


def add_graphs(a, b):
    a += b
    return a


def gather_offsets(path):
    v = Volume(path)
    return v.token_offsets()


def merge_offsets(a, b):

    keys = set()
    keys.update(list(a.keys()))
    keys.update(list(b.keys()))

    return {
        k: a.get(k, []) + b.get(k, [])
        for k in keys
    }


graph = (
    paths_dist
    .map(make_graph)
    .reduce(add_graphs)
)


print(len(graph))
