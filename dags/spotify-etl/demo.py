import sys
from random import random
from operator import add

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys
import json


def _demo():
    # print("message is: ")
    # print(sys.argv[1:])
    # print(sys.argv[2])
    print("read access keys")
    parameter = json.loads(sys.argv[1])
    # parameter = read_credential("./plugins/secrets/aws_access_key.yml")
    access_key = parameter["access_key"]
    secret_key = parameter["secret_key"]
    # it's necessary to set master value to local if you execute this program on a local machine
    spark = SparkSession\
        .builder\
        .master("local")\
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
    s3_path = "s3n://" + sys.argv[2] + "/*.csv"
    csv_rdd = spark.sparkContext.textFile(s3_path)
    c = csv_rdd.count()
    print("~~~~~~~~~~~~~~~~~~~~~count~~~~~~~~~~~~~~~~~~~~~")
    print(c)

    spark.stop()

_demo()