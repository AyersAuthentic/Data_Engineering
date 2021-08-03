import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Read and clean/process song data file. Insert processed song data into artists
    and song database tables.
    :param cur: database cuser reference
    :param filepath: path to song data json file
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True, convert_dates = False)
    
    for value in df.values:
        artist_id, artist_latitude, artist_location, artist_longitude, artist_name, duration, num_songs, song_id, title, year = value

        try:
            # insert artist record
            artist_data = (artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
            cur.execute(artist_table_insert, artist_data)
        except psycopg2.Error as e:
            print("Error: Issue inserting artist data into artist table")
            print(e)
        try:
            # insert song record
            song_data = (song_id, title, artist_id, year, duration)
            cur.execute(song_table_insert, song_data)
        except psycopg2.Error as e:
            print("Error: Issue inserting song data into song table")
            print(e)




def process_log_file(cur, filepath):
    """
    Process log and clean log file. Insert data into user, time, and songplay tables.
    :param cur: database cuser reference
    :param filepath: path to song data json file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"].astype({'ts': 'datetime64[ms]'})

    # convert timestamp column to datetime
    t = pd.Series(df['ts'], index=df.index)
    
    # insert time data records
    time_data = []
    for data in t:
        time_data.append([data, data.hour, data.day, data.weekofyear, data.month, data.year, data.day_name()])
    column_labels = ["timestamp", "hour", "day", "weekofyear", "month", "year", "weekday"]
    time_df = pd.DataFrame.from_records(data = time_data, columns = column_labels)
    
    
    for i, row in time_df.iterrows():
        try:
            cur.execute(time_table_insert, list(row))
        except psycopg2.Error as e:
            print("Error: Issue inserting data into time table")
            print(e)

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]

    # insert user records
    for i, row in user_df.iterrows():
        try:
            cur.execute(user_table_insert, row)
        except psycopg2.Error as e:
            print("Error: Issue inserting data into user table")
            print(e)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        try:
            cur.execute(song_select, (row.song, row.artist, row.length))
            results = cur.fetchone()
        except psycopg2.Error as e:
            print("Error: Issue retrieving songid/artistid from song and artist tables")
            print(e)
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = ( row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        try:
            cur.execute(songplay_table_insert, songplay_data)
        except psycopg2.Error as e:
            print("Error: Issue inserting data into songplay table")
            print(e)


def process_data(cur, conn, filepath, func):
    """
    Process and load all data into the Postgres database:
    :param cur: a database cursor reference
    :param conn: database connection reference
    :param filepath: parent directory where the files exists
    :param func: function to call
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()