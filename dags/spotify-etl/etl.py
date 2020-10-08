# -*- coding: utf-8 -*-
import sys
from random import random
from operator import add
import os

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import sys
import json
from pyspark.sql.types import DateType
from pyspark.sql.functions import to_timestamp, col, date_format

# deal with encoding error in Pyspark 
# https://stackoverflow.com/questions/39662384/pyspark-unicodeencodeerror-ascii-codec-cant-encode-character

# reload(sys)
# sys.setdefaultencoding('utf-8')

def main():
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
        .appName("DailyMusicInfo")\
        .getOrCreate()

    print("spark versio is:", spark.version)

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
    df = spark.read.load(s3_path, format="csv", sep=",", header="true", encoding="utf-8")
    print("df.show() with encoding")
    df.show()

    print("convert type of relase_date to date")
    df = df.withColumn("release_date", df["release_date"].cast(DateType()))
    df.show()

    print("format date")

    df = df.withColumn("dt_y", date_format(col("release_date"), 'yyyy-MM')) \
           .withColumn("dt_m", date_format(col("release_date"), 'yyyy-MM')) \
           .withColumn("dt_d", date_format(col("release_date"), 'yyyy-MM-dd'))

    # df = df.withColumn("release_date", df["release_date"].cast(DateType()))

    # TODO: ARRANGE THE ORDER OF COLUMNS

    print("df.show() after converting")
    df.show()

    partition_columns = ["dt_y", "dt_m", "dt_d"]

    env = os.getenv('env', 'stg')
    dst_s3 = 's3n://data-lake-' + env + '/spotify/artist-songs/'


    print("Write data to s3")

    df.write.mode('overwrite').partitionBy(partition_columns).csv(dst_s3, header=True, compression="gzip")

    # csv_rdd = spark.sparkContext.textFile(s3_path)
    # c = csv_rdd.count()
    # print("~~~~~~~~~~~~~~~~~~~~~count~~~~~~~~~~~~~~~~~~~~~")
    # print(c)

    spark.stop()

if __name__ == "__main__":
    main()