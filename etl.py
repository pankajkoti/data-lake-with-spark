import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Return a spark session by getting an existing or creating a new one
    :return:
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song data including artist data by extracting it from input S3 location, transform to dimensional tables
    and write it back to output S3 location in parquet format
    :param spark: Spark session object
    :param input_data: S3 location for song data files
    :param output_data: S3 location for writing extracted song and artist data
    :return:
    """
    # get filepath to song data file
    song_data = os.path.join(input_data, "song_data/*/*/*/*.json")

    # read song data file
    song_schema = StructType([StructField('num_songs', IntegerType()),
                              StructField('artist_id', StringType()),
                              StructField('artist_latitude', DoubleType()),
                              StructField('artist_longitude', DoubleType()),
                              StructField('artist_location', StringType()),
                              StructField('artist_name', StringType()),
                              StructField('song_id', StringType()),
                              StructField('title', StringType()),
                              StructField('duration', DoubleType()),
                              StructField('year', IntegerType())])
    df = spark.read.json(song_data, schema=song_schema)

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id', 'year', 'duration').distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_output_file = output_data + "songs.parquet"
    songs_table.write.partitionBy('year', 'artist_id').parquet(songs_output_file, mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').\
        withColumnRenamed('artist_name', 'name').\
        withColumnRenamed('artist_location', 'location').\
        withColumnRenamed('artist_latitude', 'latitude').\
        withColumnRenamed('artist_longitude', 'longitude').distinct()
    
    # write artists table to parquet files
    artists_output_file = output_data + "artists.parquet"
    artists_table.write.parquet(artists_output_file, mode='overwrite')


def process_log_data(spark, input_data, output_data):
    """
    Process event data files to extract user data, time data and building fact table data for songplays table
    by extracting it from input S3 location, transform to dimensional tables and write it back to
    output S3 location in parquet format
    :param spark: Spark session object
    :param input_data: S3 location for event data files
    :param output_data: S3 location for writing extracted users, time and fact table songplays
    :return:
    """
    # get filepath to log data file
    log_data = input_data + "log_data/"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').\
        withColumnRenamed('userId', 'user_id').\
        withColumnRenamed('firstName', 'first_name').\
        withColumnRenamed('lastName', 'last_name').\
        withColumnRenamed('gender', 'gender').\
        withColumnRenamed('level', 'level').distinct()
    
    # write users table to parquet files
    user_output_file = output_data + "users.parquet"
    users_table.write.parquet(user_output_file)

    # create timestamp column from original timestamp column
    get_timestamp = udf()
    df = 
    
    # create datetime column from original timestamp column
    get_datetime = udf()
    df = 
    
    # extract columns to create time table
    time_table = df.select()
    
    # write time table to parquet files partitioned by year and month
    time_output_file = output_data + "time.parquet"
    time_table

    # read in song data to use for songplays table
    songs_parquet_file = output_data + "songs.parquet"
    song_df = spark.read.parquet(songs_parquet_file)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table =

    # write songplays table to parquet files partitioned by year and month
    songplays_table


def main():
    """
    Orchestrate the ETL by calling functions to process song and log data
    :return:
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkify-output/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
