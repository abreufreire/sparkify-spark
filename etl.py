#!/usr/bin/env python
# -*- coding: utf-8 -*-

import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, IntegerType, DateType, TimestampType
from pyspark.sql.utils import AnalysisException

def create_spark_session():
    """
    creates new session (or uses existing one)
    :return: spark session
    """

    spark = SparkSession \
        .builder \
        .appName("sparkify-etl") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.0") \
        .getOrCreate()
    return spark


def create_spark_context(spark):
    """
    creates spark context in current spark session.
    :param spark: spark session obj
    :return: spark context obj

    notes (tuning):
    https://spark.apache.org/docs/3.1.2/cloud-integration.html#content
    https://kb.databricks.com/data/append-slow-with-spark-2.0.0.html
    https://knowledge.udacity.com/questions/467644
    """
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.algorithm.version", "2")

    return sc


def process_song_data(spark, input_data, output_data):
    """
    processes song data from S3 bucket input data (json format) path/directory & writes output to dimension tables
    (artists & songs) in S3 bucket output data (parquet format) path.
    :param spark: spark session
    :param input_data: raw data from S3 bucket
    :param output_data: ready data to S3 bucket
    :return: none
    """

    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data", "*", "*", "*", "*.json")
    # test a subset of data:
    # song_data = os.path.join(input_data, "song_data", "A", "B", "*", "*.json")

    # song schema explicitly
    song_schema = StructType([
        StructField("num_songs", IntegerType()),
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_name", StringType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("duration", DoubleType()),
        StructField("year", IntegerType())
    ])

    # read song data file
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_fields = ["title", "artist_id", "year", "duration"]
    songs_table = df.select(songs_fields).dropDuplicates().withColumn("song_id", F.monotonically_increasing_id())

    # write songs table to parquet files partitioned by year & artist
    print("\nwriting songs to parquet..")
    try:
        songs_table.write.mode("overwrite").partitionBy("year", "artist_id").parquet(os.path.join(output_data, "songs"))
        print("\nsongs done.")
    except AnalysisException as e:
        print("\nunable to write songs to parquet: {}".format(e))

    # extract columns to create artists table
    artists_fields = ["artist_id", "artist_name as name", "artist_location as location",
                      "artist_latitude as latitude", "artist_longitude as longitude"]

    artists_table = df.selectExpr(artists_fields).dropDuplicates()

    # write artists table to parquet files
    print("\nwriting artists to parquet..")
    try:
        artists_table.write.mode("overwrite").parquet(os.path.join(output_data, "artists"))
        print("\nartists done.")
    except AnalysisException as e:
        print("\nunable to write artists to parquet: {}".format(e))

    # print dataframe schema:
    # songs_table.printSchema()
    # artists_table.printSchema()


def process_log_data(spark, input_data, output_data):
    """
    processes log data from S3 bucket input data (json format) path/directory & writes output to dimension tables
    (artists & songs) in S3 bucket output data (parquet format) path.
    :param spark: spark session
    :param input_data: raw data from S3 bucket
    :param output_data: ready data to S3 bucket
    :return: none
    """

    # get filepath to log data file
    # s3 bucket:
    log_data = os.path.join(input_data, "log_data", "*", "*", "*.json")
    # local mode (data sample):
    # log_data = os.path.join(input_data, "log_data", "*.json")

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table
    users_fields = ["userId as user_id", "firstName as first_name", "lastName as last_name", "gender", "level"]
    users_table = df.selectExpr(users_fields).dropDuplicates()  # subset=["user_id"]

    # write users table to parquet files
    print("\nwriting users to parquet..")
    try:
        users_table.write.mode("overwrite").parquet(os.path.join(output_data, "users"))
        print("\nusers done.")
    except AnalysisException as e:
        print("\nunable to write users to parquet: {}".format(e))

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(x / 1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp(df.ts))

    # # create datetime column from original timestamp column
    # get_datetime = F.udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    # df = df.withColumn("start_time", get_datetime(df.timestamp))

    # get unique timestamp & extract columns to create time table
    time_table = df.select("start_time").dropDuplicates()

    time_table = time_table.select(
        F.col("start_time"),
        F.hour("start_time").alias("hour"),
        F.dayofmonth("start_time").alias("day"),
        F.weekofyear("start_time").alias("week"),
        F.month("start_time").alias("month"),
        F.year("start_time").alias("year"),
        F.dayofweek("start_time").alias("weekday")
    )

    # write time table to parquet files partitioned by year & month
    print("\nwriting time to parquet..")
    try:
        time_table.write.mode("overwrite").partitionBy("year", "month").parquet(os.path.join(output_data, "time"))
        print("\ntime done.")
    except AnalysisException as e:
        print("\nunable to write time to parquet: {}".format(e))

    # read in song data to use for songplays table
    songs_df = spark.read.parquet(os.path.join(output_data, "songs"))  # "*", "*", "*"

    artists_df = spark.read.parquet(os.path.join(output_data, "artists"))

    songs_artists_df = songs_df.join(artists_df, songs_df.artist_id == artists_df.artist_id).drop(songs_df.artist_id)

    # filter by relevant fields to songplays
    songs_artists_df = songs_artists_df.select("song_id", "title", "artist_id", F.col("name").alias("artist_name"))

    songs_artists_logs = df.join(songs_artists_df,
                                 (df.artist == songs_artists_df.artist_name) & (df.song == songs_artists_df.title))

    songplays_table = songs_artists_logs.select(F.monotonically_increasing_id().alias("songplay_id"),
                                                get_timestamp("ts").alias("start_time"),
                                                F.year("start_time").alias("year"),
                                                F.month("start_time").alias("month"),
                                                F.col("userId").alias('user_id'),
                                                F.col("level"),
                                                F.col("song_id"),
                                                F.col("artist_id"),
                                                F.col('sessionId').alias('session_id'),
                                                F.col("location"),
                                                F.col('userAgent').alias('user_agent')
                                                )

    # write songplays table to parquet files partitioned by year & month
    print("\nwriting songplays to parquet..")
    try:
        songplays_table.write.mode("overwrite").partitionBy("year", "month")\
            .parquet(os.path.join(output_data, "songplays"))
        print("\nsongplays done.")
    except AnalysisException as e:
        print("\nunable to write songlays to parquet: {}".format(e))

    # print dataframe schema:
    # users_table.printSchema()
    # time_table.printSchema()
    # songplays_table.printSchema()


def etl():

    if len(sys.argv) != 3:

        # local mode: parameters from config file dl.cfg
        config = configparser.ConfigParser()
        config.read_file(open("dl.cfg"))

        os.environ["aws_key"] = config.get("default", "aws_access_key_id")
        os.environ["aws_secret"] = config.get("default", "aws_secret_access_key")

        os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/adoptopenjdk-8.jdk/Contents/Home"
        os.environ["SPARK_HOME"] = "/usr/local/Cellar/apache-spark/3.1.2/libexec"

        # test with data sample
        # input_data = "data"
        # output_data = "parquet"

        input_data = config.get("S3", "input_data")
        output_data = config.get("S3", "output_data")

    else:
        input_data = sys.argv[1]
        output_data = sys.argv[2]

    # funcs:
    spark = create_spark_session()
    sc = create_spark_context(spark)

    process_song_data(spark, input_data, output_data)

    process_log_data(spark, input_data, output_data)

    # spark.stop()


if __name__ == "__main__":
    etl()
