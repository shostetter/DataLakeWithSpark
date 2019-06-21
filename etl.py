import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create spark session
    INPUTS:
    None
    RETURNS:
    None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Extract song data from source (AWS S3 bucket)
    Transform raw song data for song and artist tables
    Write transformed tables to AWS output S3 bucket 
    
    INPUTS:
    :spark: spark session   
    :input_data: data source location 
    :output_data: AWS S3 output location 
    
    RETURNS:
    None
    """
    # filepath to song data files
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    
    # read song data file
    print ('Reading data from S3...')
    df_song = spark.read.json(song_data)

    # create songs table
    songs_table = df_song.select("song_id","title","artist_id","year","duration").dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    print ('Writing song table parquet files to S3...')
    songs_table.write.partitionBy('year', 'artist_id').parquet(os.path.join(output_data, 'songs'), 'overwrite')
    
    # extract columns to create artists table
    artists_table = df_song.selectExpr("artist_id", 
                              "artist_name",
                              "artist_location as location", 
                              "artist_latitude as lat", 
                              "artist_longitude as lon").dropDuplicates()
    
    # write artists table to parquet files
    print ('Writing artists table parquet files to S3...')
    artists_table.write.parquet("{}/artists".format(output_data))


def process_log_data(spark, input_data, output_data):
    """
    Extract log data from source (AWS S3 bucket)
    Transform raw log and song data for time and songplays tables
    Write transformed tables to AWS output S3 bucket 
    
    INPUTS:
    :spark: spark session   
    :input_data: data source location 
    :output_data: AWS S3 output location 
    
    RETURNS:
    None
    """
    # filepath to log data file
    #log_data ="{}/log_data/*.json".format(input_data) # this path for sample data only
    log_data ="{}/log_data/*/*/*.json".format(input_data)
    

    # read log data file
    df_log = spark.read.json(log_data)
    
    # extract columns for users table - filtered for song plays
    users_table = df_log.filter(df_log.page == 'NextSong').selectExpr(
        "userid",
        "firstname",
        "lastname",
        "gender",
        "level").dropDuplicates()
    
    # write users table to parquet files
    print ('Writing user table parquet files to S3...')
    users_table.write.parquet("{}/users".format(output_data))

    # set up UDFs for parsing datetime
    # datetime
    get_datetime = udf(lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S'))
    df_log = df_log.withColumn("time_stamp", get_datetime(df_log.ts))
    #hour
    get_hour = udf(lambda x: datetime.fromtimestamp(x / 1000.0).hour)
    df_log = df_log.withColumn("hour", get_hour(df_log.ts))
    #day
    get_day = udf(lambda x: datetime.fromtimestamp(x / 1000.0).day)
    df_log = df_log.withColumn("day", get_day(df_log.ts))
    #month
    get_month = udf(lambda x: datetime.fromtimestamp(x / 1000.0).month)
    df_log = df_log.withColumn("month", get_month(df_log.ts))
    #year
    get_year = udf(lambda x: datetime.fromtimestamp(x / 1000.0).year)
    df_log = df_log.withColumn("year", get_year(df_log.ts))
    # weekday
    get_dow = udf(lambda x: datetime.fromtimestamp(x / 1000.0).weekday())
    df_log = df_log.withColumn("dow", get_dow(df_log.ts))
    
    # extract columns to create time table
    time_table = df_log.selectExpr("ts", "time_stamp", "hour","day","month","year", "dow").dropDuplicates()
    
    # write time table to parquet files partitioned by year and month
    print ('Writing time table parquet files to S3...')
    time_table.write.partitionBy("year", "month").parquet("{}/time".format(output_data))

    # read in song data to use for songplays table
    print ('Reading song data from S3...')
    song_data = "{}/song_data/*/*/*/*.json".format(input_data)
    song_df = spark.read.json(song_data)
    
    # join song and log data dataframes 
    songplays = df_log.join(song_df, df_log.artist==song_df.artist_name)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays.selectExpr("ts", "artist_id", "song_id", "sessionid",
                                           "location","useragent","userid").withColumn(
        "time_stamp", get_datetime(df_log.ts)).withColumn(
        "year", get_year(df_log.ts)).withColumn(
        "month", get_month(df_log.ts)).withColumn(
        "songplay_id", monotonically_increasing_id()).dropDuplicates()

    # write songplays table to parquet files partitioned by year and month
    print ('Writing songplays table parquet files to S3...')
    songplays_table.write.partitionBy("year", "month").parquet("{}/songplays".format(output_data))


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://sparkdatalake"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
