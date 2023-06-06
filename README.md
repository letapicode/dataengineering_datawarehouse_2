# Data Engineering Report: Sparkify Data Warehouse and ETL Pipeline


## Introduction
Sparkify, a popular music streaming startup, has been experiencing rapid growth in its user base 
and song library. To better understand user behavior and preferences, Sparkify has collected log 
data and song metadata in JSON format. The company's data engineering team was tasked with designing 
a data warehouse and an ETL pipeline to consolidate this data and facilitate efficient analysis.

This report details the design, implementation, and testing of a scalable and maintainable data 
warehouse using Amazon Redshift and an ETL pipeline using Python.


## Project Overview
The primary goals of this project were to:

Design and implement a scalable and maintainable data warehouse on Amazon Redshift to store 
Sparkify's user activity and song metadata.
Develop an ETL pipeline to extract raw data from Amazon S3, transform it into a structured 
format, and load it into the data warehouse.

To achieve these goals, the following AWS services were utilized:

Amazon S3: To store the raw log data and song metadata.
Amazon Redshift: To host the data warehouse.
AWS Identity and Access Management (IAM): To manage access permissions for Redshift to access 
the data stored in Amazon S3.


## Data Insights

### The staging_events table:
The staging_events columns include artist, auth, firstName, gender, itemInSession, lastName, length, 
level, location, method, page, registration, sessionId, song, status, ts, userAgent, and userId. 
These columns contain various information about user interactions, such as playing or skipping a song, 
searching for a new song, or upgrading to a paid subscription.

- artist: This column contains the name of the artist associated with the event, such as a
            song being played or skipped.

- auth: This column indicates whether the user is logged in or out of the app when the event occurred.

- firstName: This column contains the first name of the user associated with the event.

- gender: This column contains the gender of the user associated with the event, indicated by either 
            "M" for male or "F" for female.

- itemInSession: This column records the sequential order of each activity in a session. For instance, if a 
        user plays a song, searches for another one, and plays it, each action receives a unique value 
        in the itemInSession column, indicating its order. Analysts can use this column to track user 
        actions and their relationship to each other over time within a session. 

- lastName: This column contains the last name of the user associated with the event.

- length: This column contains the length of the song played during the event, in seconds.

- level: This column indicates whether the user has a paid or free subscription.

- location: This column contains the location of the user associated with the event, such as a city and state.

- method: This column indicates whether the HTTP request method used for the event was a PUT or GET.

- page: This column represents the current page the user is on within the app, such as "NextSong" or "Home". 
        "NextSong" is the default page when a user starts playing a song, indicating the intention to 
        listen to music. After the first song, the page value may change based on user interactions with 
        the app, such as skipping to the next song or pausing the music, which may change the page value 
        to "NextSong", "Home", or "Logout". 

- registration: This column contains the timestamp of when the user was registered for the app, in epoch time.

- sessionId: This column contains a unique identifier for each session.

- song: This column contains the name of the song associated with the event, such as a song being played or skipped.

- status: This column indicates the HTTP status code for the event, such as 200 for success or 404 for not found.

- ts: This column contains the timestamp of the event itself, in epoch time.

- userAgent: This column contains the user agent string of the application used by the user.

- userId: This column contains a unique identifier for each user associated with the event.


## The staging_songs table:
The staging_songs columns include song_id, num_songs, title, artist_name, artist_latitude, year, duration, 
artist_id, artist_longitude, and artist_location. These columns contain information about the songs and 
artists available in the Sparkify library.

- song_id: This column contains a unique identifier for each song.

- num_songs: This column contains the number of songs in the dataset, which is always 1 for this data.

- title: This column contains the title of the song.

- artist_name: This column contains the name of the artist associated with the song.

- artist_latitude: This column contains the latitude of the artist's location.

- year: This column contains the release year of the song.

- duration: This column contains the duration of the song, in seconds.

- artist_id: This column contains a unique identifier for each artist associated with the song.

- artist_longitude: This column contains the longitude of the artist's location.

- artist_location: This column contains the location of the artist, such as a city and state.


## Data Warehouse Design
A star schema design was chosen for the data warehouse due to its simplicity, performance advantages, 
and ease of use for analytical queries. The schema consists of a central fact table surrounded by dimension tables.


## Fact table:
songplays: Stores records of each song play event, including details about the user, song, artist, and timestamp.
    
    {songplay_id, start_time, user_id,level, song_id, artist_id, session_id, location, user_agent}


## Dimension tables:
users: Stores information about users, such as user ID, first name, last name, gender, and level.

    {user_id, first_name, last_name, gender, level}

songs: Stores information about songs, such as song ID, title, artist ID, year, and duration.

    {song_id, title, artist_id, year, duration} 

artists: Stores information about artists, such as artist ID, name, location, latitude, and longitude.

    {artist_id, name, location, latitude, longitude}


time: Stores timestamps of song play events, broken down into hour, day, week, month, year, and weekday.

    {start_time, hour, day, week, month, year, weekday}



## ETL Pipeline
The ETL pipeline was designed to extract raw data from Amazon S3, stage the data 
in Redshift, and transform it into the star schema. The pipeline was implemented 
using Python and consists of the following steps:

Extract: Load raw data from Amazon S3 into Redshift staging tables (staging_events and staging_songs).

Transform: Transform the data and insert it into the fact and dimension tables. Deduplicate records,
            handle missing values, and ensure data integrity.
            
Load: Load the transformed data into the data warehouse's fact and dimension tables.


## Testing and Validation
To ensure data integrity and the accuracy of the ETL pipeline, extensive testing was 
performed throughout the development process:

Schema Validation: Confirm that table structures match the intended design and that data types are appropriate.

Data Consistency: Check that the data in the data warehouse is consistent with the raw data in Amazon S3.

Data Integrity: Validate that primary and foreign key relationships are correctly enforced, and 
that data is deduplicated and free of anomalies.

Performance: Test query performance on the data warehouse to ensure that the schema design supports efficient querying.



## Example Queries
The data warehouse facilitates a wide range of analytical queries, such as:

Most played songs: Identify the most popular songs by counting the number of plays for each song.
Peak usage hours: Determine the hours during which users are most active by aggregating song plays by hour.
User demographics: Analyze user demographics by aggregating data on gender, location, and subscription level.


Here are a few example queries, to see results, please run the etl.py file:

## Most Played Songs
SELECT s.title, a.name, COUNT(*) as play_count

FROM songplays sp

JOIN songs s ON sp.song_id = s.song_id

JOIN artists a ON sp.artist_id = a.artist_id

GROUP BY s.title, a.name

ORDER BY play_count DESC

LIMIT 10;



## Peak Usage Hours
SELECT t.hour, COUNT(*) as play_count

FROM songplays sp

JOIN time t ON sp.start_time = t.start_time

GROUP BY t.hour

ORDER BY play_count DESC;


## User Demographics
SELECT u.gender, u.level, COUNT(DISTINCT u.user_id) as user_count

FROM songplays sp

JOIN users u ON sp.user_id = u.user_id

GROUP BY u.gender, u.level

ORDER BY user_count DESC;


These queries provide valuable insights into user behavior and preferences, enabling Sparkify to 
make data-driven decisions and improve its services.

## Conclusion
In this project, I successfully designed and implemented a scalable and maintainable data warehouse 
on Amazon Redshift and an ETL pipeline using Python. The star schema design facilitates efficient querying, 
while the ETL pipeline ensures data consistency and integrity.

The data warehouse enables Sparkify to analyze user behavior and preferences, make data-driven decisions, 
and enhance its services. As the company continues to grow, the data warehouse and ETL pipeline can be 
easily scaled to handle increased data volumes and user activity.

## 

# A Day in the Life of a Sparkify User: The Data Story

Meet Udacia, an avid music lover and a Sparkify user. As she wakes up and gets ready for the day, she opens the Sparkify app on her smartphone to listen to her favorite songs. Little does she know, her actions on the app contribute to valuable data that helps Sparkify improve its services and understand its users better.

When Udacia starts playing her first song by the artist "The Beatles," an event is created in the staging_events table with details such as the artist name, song title, timestamp (ts), session ID, user agent, and more. The page column is set to "NextSong," indicating that Udacia has started listening to music. Her userId, firstName, lastName, and gender are also captured, helping Sparkify understand its user demographics.

As Udacia skips to the next song, another event is generated, updating the itemInSession column to indicate the order of her actions within the session. This information helps Sparkify understand user behavior and track the flow of interactions during a session. Additionally, the method and status columns record the HTTP request method and status code associated with this action.

While listening to music, Udacia decides to upgrade her subscription from free to paid. This change is reflected in the level column of the staging_events table, and subsequently, in the users dimension table. By analyzing changes in subscription levels, Sparkify can gain insights into user preferences and the value they place on the service.

Later in the day, Udacia listens to a new song she discovers on Sparkify, titled "New Horizons" by the artist "Solar Winds." The song's details, such as song_id, title, artist_name, duration, year, and artist_location, are stored in the staging_songs table. By analyzing this data, Sparkify can identify popular songs and tailor its recommendations to users based on their preferences.

Throughout the day, Udacia's listening habits contribute to the data used for the "Peak Usage Hours" and "User Demographics" example queries. Her registration timestamp helps Sparkify understand when users join the platform, and her location data allows the company to analyze user behavior by geographic region.

Behind the scenes, as a diligent data engineer for Sparkify, I ensure that all the data generated by users like Udacia is efficiently stored, processed, and analyzed. Through the use of Amazon Redshift, a star schema design, and a robust ETL pipeline, I've built a data warehouse for Sparkify that is well-equipped to handle growing data volumes and user activity.

As Udacia continues to use Sparkify to discover and enjoy her favorite music, her actions help Sparkify make data-driven decisions, improve its services, and ultimately, create a better music streaming experience for users like her.



S3 Data Links:
--
Log-Data:
https://s3-us-west-2.amazonaws.com/udacity-dend/log-data/2018/11/2018-11-05-events.json

Song-Data:
https://s3-us-west-2.amazonaws.com/udacity-dend/song-data/A/A/A/TRAAAEA128F935A30D.json


Log_json_path:
https://s3-us-west-2.amazonaws.com/udacity-dend/log_json_path.json

