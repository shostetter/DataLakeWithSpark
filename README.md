# Sparkify: Data Lake
## Background:
The music streaming startup, Sparkify, has been growning their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app. This project builds an ETL pipeline that extracts the data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables for the analytics team. 
### Raw Datasets
**Song Dataset**
The song data is from the Million Song Dataset. Each file is in JSON format and contains metadata about a song and the artist of that song. The files are partitioned by the first three letters of each song's track ID. For example, here are filepaths to two files in this dataset.
- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json  
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.  

`{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "",
"artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`
  
**Log Dataset**
The Spariky streaming app generates activity logs partitioned by year and month. For example, here are filepaths to two files in this dataset.
- log_data/2018/11/2018-11-12-events.json
- log_data/2018/11/2018-11-13-events.json
  
And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.  
<img src="log-data.png">  

## Schema for Song Play Analysis
In this project the song and log datasets are transformed into a star schema optimized for queries on song play analysis. This includes the following tables.

**Fact Table**
 1. **songplays** - records in log data associated with song plays i.e. records with page **NextSong**
  - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*
  
**Dimension Tables**
 1. **users** - users in the app
  - *user_id, first_name, last_name, gender, level*
 1. **songs** - songs in music database
  - *song_id, title, artist_id, year, duration*
 1. **artists** - artists in music database
  - *artist_id, name, location, lattitude, longitude*
 1. **time** - timestamps of records in **songplays** broken down into specific units
  - *start_time, hour, day, week, month, year, weekday*
  
# ETL pipeline 
This pipeline extracts data from S3, stages the data in a data lake, and transforms the data into a set of dimensional tables writing back to S3 for the analytics team to continue finding insights in what songs the users are listening to. 

## Running the ETL pipeline
1. Add AWS key and secret to the configuration file dl.cfg 
1. Set up an S3 bucket for the output data. 
1. run the etl.py file

# Data exploration
A small sample of the data is provided in the data folder, and the ETL_SampleData.ipynb notebook has been set up to process the sample data. The notebook includes insights into the schema and sample records of the data throughout the process as well as a few sample analytics queries. 