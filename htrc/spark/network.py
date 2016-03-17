

from pyspark import SparkContext, SparkConf

from htrc.corpus import Corpus
from htrc.volume import Volume


conf = SparkConf().setAppName('network').setMaster('local')
sc = SparkContext(conf=conf)


# Wrap path collection.
paths = Corpus('data/basic').paths()
paths_dist = sc.parallelize(paths)


def count_tokens(path):
    v = Volume(path)
    return v.token_count


count = (
    paths_dist
    .map(count_tokens)
    .reduce(lambda a, b: a + b)
)


print(count)
