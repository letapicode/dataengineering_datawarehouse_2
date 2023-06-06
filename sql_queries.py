"""
The sql_queries.py file contains SQL statements that define the tables to be created, as well as queries for inserting data into the tables and for querying the data. These queries are defined as Python strings, and they can be imported into other Python scripts, such as create_tables.py and etl.py, using the import statement.

This script contains SQL queries for creating, dropping, copying, and inserting data into tables in a Redshift cluster. It also includes queries to count the number of records in each table and queries to extract insights from the data.

The script starts by importing the configparser library and reading the configuration file. It then defines variables for dropping the tables and creating the tables. The CREATE TABLES section contains queries to create staging_events, staging_songs, songplays, users, songs, artists, and time tables.

After the CREATE TABLES section, there are queries for loading data into the staging tables using the COPY command. Then there are queries for inserting data into the final tables, which include songplays, users, songs, artists, and time.

The script also includes queries for counting the number of records in each table, which are useful for verifying the completeness of data. It further includes queries for extracting insights from the data, which include the most played songs, peak usage hours, and user demographics.

Finally, the script defines lists that contain the queries to create, drop, copy, insert, count, and extract insights from tables.

"""
import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR,
    itemInSession   INTEGER,
    lastName        VARCHAR,
    length          FLOAT,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    BIGINT,
    sessionId       INTEGER,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          INTEGER
);
""")


staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id         VARCHAR,
    num_songs       INTEGER,
    title           VARCHAR,
    artist_name     VARCHAR,
    artist_latitude FLOAT,
    year            INTEGER,
    duration        FLOAT,
    artist_id       VARCHAR,
    artist_longitude FLOAT,
    artist_location VARCHAR
);
""")


songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id     INTEGER IDENTITY(0,1) PRIMARY KEY,
    start_time      TIMESTAMP NOT NULL,
    user_id         INTEGER NOT NULL,
    level           VARCHAR,
    song_id         VARCHAR,
    artist_id       VARCHAR,
    session_id      INTEGER,
    location        VARCHAR,
    user_agent      VARCHAR
);
""")


user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id         INTEGER PRIMARY KEY,
    first_name      VARCHAR,
    last_name       VARCHAR,
    gender          VARCHAR,
    level           VARCHAR
);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id         VARCHAR PRIMARY KEY,
    title           VARCHAR,
    artist_id       VARCHAR,
    year            INTEGER,
    duration        FLOAT
);
""")


artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id       VARCHAR PRIMARY KEY,
    name            VARCHAR,
    location        VARCHAR,
    latitude        FLOAT,
    longitude       FLOAT
);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time      TIMESTAMP PRIMARY KEY,
    hour            INTEGER,
    day             INTEGER,
    week            INTEGER,
    month           INTEGER,
    year            INTEGER,
    weekday         INTEGER
);
""")



# STAGING TABLES
"""
These are COPY statements used to load data from S3 into the corresponding Redshift tables.

In the staging_events_copy query, the COPY command loads data from the log_data JSON files in S3 
to the staging_events table. The CREDENTIALS option is used to specify the AWS IAM role that 
allows Redshift to access the S3 bucket, and the FORMAT option specifies the JSON format of the data.

In the staging_songs_copy query, the COPY command loads data from the song_data JSON files in S3 
to the staging_songs table. The CREDENTIALS and FORMAT options are specified in a similar way as 
in the staging_events_copy query.
"""

staging_events_copy = ("""
COPY staging_events 
FROM 's3://udacity-dend/log_data'
CREDENTIALS 'aws_iam_role=arn:aws:iam::065061674598:role/myDataWarehouseRedshiftRole'
REGION 'us-west-2'
FORMAT AS JSON {};
""").format(config.get('S3', 'log_jsonpath'))

staging_songs_copy = ("""
COPY staging_songs 
FROM 's3://udacity-dend/song_data'
CREDENTIALS 'aws_iam_role=arn:aws:iam::065061674598:role/myDataWarehouseRedshiftRole'
REGION 'us-west-2'
FORMAT AS JSON 'auto';
""")





# FINAL TABLES
"""
This SQL statement inserts data into the songplays table. The data is selected from the staging_events 
and staging_songs tables, which were populated by the COPY commands in the previous section. The SELECT 
statement uses a join on the song and artist columns to match records between the two staging tables. 
The columns selected are mapped to the columns in the songplays table, with additional expressions used 
to transform the ts column from Unix timestamp to Redshift TIMESTAMP data type. The WHERE clause limits 
the records to those where the page column is 'NextSong', which represents records for actual song plays 
as opposed to other page events in the logs, and only allows users that are not null. 

"""
songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT
    TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' AS start_time,
    e.userId AS user_id,
    e.level,
    s.song_id,
    s.artist_id,
    e.sessionId AS session_id,
    e.location,
    e.userAgent AS user_agent
FROM staging_events e
JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration
WHERE e.page = 'NextSong' AND e.userId IS NOT NULL;
""")



"""
This query is inserting data into the users table using the INSERT INTO statement. It is 
selecting the data from the staging_events table, specifically selecting the DISTINCT userId, 
firstName, lastName, gender, and level columns. The WHERE clause filters the data by only 
selecting rows where userId is not NULL and the page is 'NextSong'. The selected data is then inserted into the users table.
"""
user_table_insert = ("""
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL AND page = 'NextSong';
""")



"""
This SQL statement inserts data into the songs table from the staging_songs table. 
It selects the columns song_id, title, artist_id, year, and duration from the 
staging_songs table and inserts them into the songs table. The DISTINCT keyword is 
used to ensure that only unique rows are inserted.
"""
song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs;
""")



"""
This is an SQL query that inserts data into the artists table of the database. It selects 
distinct values of artist_id, artist_name, artist_location, artist_latitude, and artist_longitude 
from the staging_songs table, and inserts them into the corresponding columns in the artists table. 
The DISTINCT keyword is used to ensure that only unique values are inserted into the artists table. 
This query is part of the ETL process and it populates the artists table with data from the staging_songs table.
"""
artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")



"""
This SQL statement inserts data into the time table by extracting and transforming various time-related values from the start_time column in the songplays table.

Each row in the songplays table represents a specific event when a user started playing a song, and start_time is the timestamp of that event.

The INSERT INTO statement specifies the target table time and the columns that will be populated with data.

The SELECT statement retrieves data from the songplays table and uses several EXTRACT functions to obtain the hour, day, week, month, year, and weekday values from the start_time column.

These values are then inserted into the corresponding columns in the time table.

"""
time_table_insert = ("""
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT
    start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEK FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    EXTRACT(WEEKDAY FROM start_time) AS weekday
FROM songplays;
""")



# Count number of records in each table
count_staging_events = ("""
    SELECT COUNT(*) FROM staging_events
""")

count_staging_songs = ("""
    SELECT COUNT(*) FROM staging_songs
""")

count_songplays = ("""
    SELECT COUNT(*) FROM songplays
""")

count_users = ("""
    SELECT COUNT(*) FROM users
""")

count_songs = ("""
    SELECT COUNT(*) FROM songs
""")

count_artists = ("""
    SELECT COUNT(*) FROM artists
""")

count_time = ("""
    SELECT COUNT(*) FROM time
""")


#Insights
"""
The most_played_songs_query selects the top 10 most played songs by joining the songplays 
table with the songs and artists tables to get the song and artist names, and grouping by 
those fields while counting the number of times each song was played.
"""
most_played_songs_query = ("""
SELECT s.title, a.name, COUNT(*) as play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
JOIN artists a ON sp.artist_id = a.artist_id
GROUP BY s.title, a.name
ORDER BY play_count DESC
LIMIT 10;
""")


"""
The peak_usage_hours_query selects the most popular hours for song plays by joining the 
songplays table with the time table to get the hour of each song play, grouping by hour, 
and counting the number of song plays per hour.
"""
peak_usage_hours_query = ("""
SELECT t.hour, COUNT(*) as play_count
FROM songplays sp
JOIN time t ON sp.start_time = t.start_time
GROUP BY t.hour
ORDER BY play_count DESC;
""")


"""
The user_demographics_query selects the count of users by gender and level (paid or free) 
by joining the songplays table with the users table to get the gender and level of each 
user who played a song, grouping by gender and level, and counting the number of distinct 
users in each group.
"""
user_demographics_query = ("""
SELECT u.gender, u.level, COUNT(DISTINCT u.user_id) as user_count
FROM songplays sp
JOIN users u ON sp.user_id = u.user_id
GROUP BY u.gender, u.level
ORDER BY user_count DESC;
""")



# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

count_table_queries = [count_staging_events, count_staging_songs, count_songplays, count_users, count_songs, count_artists, count_time]
insights_table_queries = [most_played_songs_query, peak_usage_hours_query, user_demographics_query]
