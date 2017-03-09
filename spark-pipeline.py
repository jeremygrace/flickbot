#!/usr/bin/python
from pyspark import SparkConf, SparkContext


def deploy_spark():
    pass


def load_dataframes():
    pass


def perform_joins():
    pass


def send_parquets():
    pass


def send_to_RDS():
    pass


if __name__ == '__main__':
    conf = SparkConf() \
        .setAppName('Flickbot') \
        .set('spark.executor.memory', '2g') \
        .set('spark.master', 'local[4]')
    sc = SparkContext(conf)
