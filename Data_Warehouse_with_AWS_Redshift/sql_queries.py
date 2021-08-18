import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('cluster.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE  IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events
(
    artist VARCHAR,
    auth VARCHAR,
    firstName VARCHAR(50),
    gender CHAR,
    itemInSession INTEGER,
    lastName VARCHAR(50),
    length FLOAT,
    level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration FLOAT,
    sessionId INTEGER,
    song VARCHAR,
    status INTEGER,
    ts BIGINT,
    userAgent VARCHAR,
    userId INTEGER
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs
(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude FLOAT,
    artist_longitude FLOAT,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration FLOAT,
    year FLOAT
);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays
(
    songplay_id INT IDENTITY (1, 1) PRIMARY KEY ,
    start_time TIMESTAMP,
    user_id INT,
    level VARCHAR,
    song_id VARCHAR NOT NULL,
    artist_id VARCHAR,
    session_id INT NOT NULL,
    location VARCHAR,
    user_agent VARCHAR
)
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY ( start_time );
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users
(
    userId INT PRIMARY KEY,
    firsname VARCHAR(50),
    lastname VARCHAR(50),
    gender CHAR(1) ENCODE BYTEDICT,
    level VARCHAR ENCODE BYTEDICT NOT NULL
)
SORTKEY (userId);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs
(
    song_id VARCHAR PRIMARY KEY,
    title VARCHAR,
    artist_id VARCHAR,
    year INT ENCODE BYTEDICT,
    duration FLOAT
)
SORTKEY (song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists
(
    artist_id VARCHAR PRIMARY KEY ,
    name VARCHAR,
    location VARCHAR,
    latitude FLOAT,
    longitude FLOAT
)
SORTKEY (artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time
(
    start_time  TIMESTAMP PRIMARY KEY ,
    hour INT NOT NULL,
    day INT NOT NULL,
    week INT NOT NULL,
    month INT NOT NULL,
    year INT NOT NULL ENCODE BYTEDICT ,
    weekday VARCHAR(9) NOT NULL ENCODE BYTEDICT
)
DISTSTYLE KEY
DISTKEY ( start_time )
SORTKEY (start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS json {};
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs
FROM {}
CREDENTIALS 'aws_iam_role={}'
region 'us-west-2'
FORMAT AS json 'auto';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (START_TIME, USER_ID, LEVEL, SONG_ID, ARTIST_ID, SESSION_ID, LOCATION, USER_AGENT)
SELECT DISTINCT
       TIMESTAMP 'epoch' + (e.ts / 1000) * INTERVAL '1 second' as start_time,
                e.userId,
                e.level,
                s.song_id,
                s.artist_id,
                e.sessionId,
                e.location,
                e.userAgent
FROM staging_songs s
INNER JOIN staging_events e
ON (s.title = e.song AND e.artist = s.artist_name)
AND se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
SELECT DISTINCT userId, firstName, lastName, gender, level
FROM staging_events
WHERE userId IS NOT NULL
AND page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
SELECT
    DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artists
SELECT
    DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs;
""")

time_table_insert = ("""
insert into time
SELECT DISTINCT
       TIMESTAMP 'epoch' + (ts/1000) * INTERVAL '1 second' as start_time,
       EXTRACT(HOUR FROM start_time) AS hour,
       EXTRACT(DAY FROM start_time) AS day,
       EXTRACT(WEEKS FROM start_time) AS week,
       EXTRACT(MONTH FROM start_time) AS month,
       EXTRACT(YEAR FROM start_time) AS year,
       to_char(start_time, 'Day') AS weekday
FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
