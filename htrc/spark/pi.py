

import random

from pyspark import SparkContext, SparkConf


conf = SparkConf().setAppName('pi').setMaster('local')
sc = SparkContext(conf=conf)


SAMPLES=1000000


def sample(p):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y < 1 else 0


count = (
    sc.parallelize(range(0, SAMPLES))
    .map(sample)
    .reduce(lambda a, b: a + b)
)


print(4.0 * count / SAMPLES)
