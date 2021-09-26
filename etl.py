import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from config import user, password, host, port
from copy_df_to_table import copy_from_stringio

def process_song_file(cur, filepath):
    """
    Given a cursor and a filepath to a .json song file,
    inserts data into the 'songs' and 'artists' tables
    of our star-schema.
    :param cur: postgreSQL cursor
    :param filepath: path to .json file to insert
    :return: None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[song_cols].values
    for row in song_data:
        cur.execute(song_table_insert, tuple(row))
    
    # insert artist record
    artist_data = df[['artist_id',
                      'artist_name',
                      'artist_location',
                      'artist_latitude',
                      'artist_longitude']].values
    for row in artist_data:
        cur.execute(artist_table_insert, tuple(row))


def process_log_file(cur, filepath):
    """
    Given a cursor and a filepath to a .json log file,
    inserts data into the 'time', 'users' and 'songplays
    tables of our star-schema.
    :param cur: postgreSQL cursor
    :param filepath: path to .json file to insert
    :return: None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = t
    # insert time data records
    time_df = pd.DataFrame()
    time_df['start_time'] = t
    time_df = time_df[~time_df['start_time'].isnull()]
    time_df['hour'] = t.apply(lambda d: d.hour)
    time_df['day'] = t.apply(lambda d: d.day)
    time_df['week'] = t.apply(lambda d: d.week)
    time_df['month'] = t.apply(lambda d: d.month)
    time_df['year'] = t.apply(lambda d: d.year)
    time_df['weekday'] = t.apply(lambda d: d.weekday())

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId',
                  'firstName',
                  'lastName',
                  'gender',
                  'level']]
    user_df = user_df[~user_df['userId'].isnull()]
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    df = df[~(df['userId'].isnull() | df['ts'].isnull())]
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts,
                         row.userId,
                         row.level,
                         songid,
                         artistid,
                         row.sessionId,
                         row.location,
                         row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Iterate through all .json files to inserts into
    our schema's tables. Call function 'func'
    to insert the data (this function is passed by the user
    based on whether the filepath leads to a 'logs' file
    or a 'songs' file.
    :param cur: postgreSQL cursor
    :param conn: postgreSQL connection
    :param filepath: path to .json file to insert
    :param func: function that will insert the content of the file into tables.
    :return: None
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
    """
    Read song_data and log_data files and insert contents into the
    tables of our star-schema.
    :return: None
    """
    conn = psycopg2.connect((f"host={host} port={port} "
                             f"user={user} password={password}"
                              " dbname=sparkifydb"))
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()