#!/usr/bin/python
from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    conf = SparkConf() \
        .setAppName('Flickbot') \
        .set('spark.executor.memory', '2g') \
        .set('spark.master', 'local[4]')
    sc = SparkContext(conf)
