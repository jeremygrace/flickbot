#! /usr/bin/python
import os
import yaml
import pandas
import psycopg2
import numpy as np
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


def perform_joins(movies, trailers):
    return movies.join(trailers, ['title'])


def send_to_RDS(credentials, movies, tablename):
    conn = psycopg2.connect(
                            database=credentials['postgres_bot'].get('dbase'),
                            user=credentials['postgres_bot'].get('user'),
                            host=credentials['postgres_bot'].get('host'),
                            port=credentials["postgres_bot"].get("port"),
                            password=credentials['postgres_bot'].get('pass'))
    cur = conn.cursor()
    mov = movies.toPandas()
    cur.execute("DELETE FROM " + tablename)
    conn.commit()

    mov.fillna(value=np.nan, inplace=True)
    for i in range(len(mov)):
        m = mov.iloc[i].values
        cur.execute("INSERT INTO " + tablename + " (title, genre, rated, url,\
                    timestamp) VALUES ('{}', '{}', '{}', '{}', '{}')".format(*m))
        conn.commit()


if __name__ == '__main__':
    conf = SparkConf().setAppName('Flickbot')
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    ss = SparkSession.builder \
    .master("local") \
    .appName("Flickbot") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
    credentials = yaml.load(open(os.path.expanduser('~/admin/bot-creds.yaml')))
    up = ss.read.json("s3a://flickbot-api/upcoming.json")
    now = ss.read.json("s3a://flickbot-api/now_playing.json")
    coming_trailers = ss.read.json("s3a://flickbot-api/coming_trailers.json")
    now_trailers = ss.read.json("s3a://flickbot-api/now_trailers.json")
    in_theaters = perform_joins(now, now_trailers)
    coming_soon = perform_joins(up, coming_trailers)
    send_to_RDS(credentials, in_theaters, 'in_theaters')
    send_to_RDS(credentials, coming_soon, 'coming_soon')
