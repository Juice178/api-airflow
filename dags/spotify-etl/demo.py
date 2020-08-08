import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

def _demo():
    access_key = "******"
    secret_key = "*******"
    spark = SparkSession\
        .builder\
        .appName("Demo")\
        .getOrCreate()

    sc = spark.sparkContext

    # remove this block if use core-site.xml and env variable
    sc._jsc.hadoopConfiguration().set("fs.s3.awsAccessKeyId", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
    sc._jsc.hadoopConfiguration().set("fs.s3.awsSecretAccessKey", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
    sc._jsc.hadoopConfiguration().set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3.S3FileSystem")

    # fetch from s3, returns RDD
    csv_rdd = spark.sparkContext.textFile("s3n://data-lake-stg/spotify/top50/dt_y=2020/dt_m=2020-08/dt_d=2020-08-03/top10_popular_songs_of_artists-JP.csv")
    c = csv_rdd.count()
    print("~~~~~~~~~~~~~~~~~~~~~count~~~~~~~~~~~~~~~~~~~~~")
    print(c)

    spark.stop()