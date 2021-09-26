# DROP TABLES

songplay_table_drop = "drop table if exists songplays"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists songs"
artist_table_drop = "drop table if exists artists"
time_table_drop = "drop table if exists time"

# CREATE TABLES

songplay_table_create = ("""
create table if not exists songplays(
  songplay_id serial,
  start_time timestamp not null,
  user_id int not null,
  level varchar,
  song_id varchar,
  artist_id varchar,
  session_id int,
  location varchar,
  user_agent varchar,
  PRIMARY KEY (songplay_id))
""")

user_table_create = ("""
create table if not exists users(
  user_id int,
  first_name varchar,
  last_name varchar,
  gender varchar,
  level varchar,
  PRIMARY KEY (user_id))
""")

song_table_create = ("""
create table if not exists songs(
  song_id varchar,
  title varchar,
  artist_id varchar  not null,
  year int,
  duration float,
  PRIMARY KEY (song_id))
""")

artist_table_create = ("""
create table if not exists artists(
  artist_id varchar,
  name varchar,
  location varchar,
  latitude float DEFAULT NULL,
  longitude float DEFAULT NULL,
  PRIMARY KEY (artist_id)
)
""")

time_table_create = ("""
create table if not exists time(
  start_time timestamp,
  hour int,
  day int,
  week int,
  month int,
  year int,
  weekday int,
  PRIMARY KEY (start_time))
""")

# INSERT RECORDS

songplay_cols = ['start_time',
                 'user_id',
                 'level',
                 'song_id',
                 'artist_id',
                 'session_id',
                 'location',
                 'user_agent']
songplay_table_insert = (
  f"""
insert into songplays(
  {','.join(songplay_cols)}) values
  ({('%s,'*(len(songplay_cols)-1)) + '%s'} )
  
ON CONFLICT (songplay_id) 
DO NOTHING
"""
)

user_cols = ['user_id',
             'first_name',
             'last_name',
             'gender',
             'level']
user_table_insert = (
  f"""
insert into users(
  {','.join(user_cols)}) values
  ({('%s,'*(len(user_cols)-1)) + '%s'} )
  
ON CONFLICT (user_id) 
DO UPDATE 
  SET 
  gender = EXCLUDED.gender,
  level = EXCLUDED.level
"""
)

song_cols = ['song_id',
             'title',
             'artist_id',
             'year',
             'duration']
song_table_insert = (
  f"""
insert into songs(
  {','.join(song_cols)}) values
  ({('%s,'*(len(song_cols)-1)) + '%s'} )
  
ON CONFLICT (song_id) 
DO NOTHING
"""
)

artist_cols = ['artist_id',
               'name',
               'location',
               'latitude',
               'longitude']
artist_table_insert = (
  f"""
insert into artists(
  {','.join(artist_cols)}) values
  ({('%s,'*(len(artist_cols)-1)) + '%s'} )
  
ON CONFLICT (artist_id) 
DO NOTHING
  
"""
)

time_cols = ['start_time',
             'hour',
             'day',
             'week',
             'month',
             'year',
             'weekday']
time_table_insert = (
  f"""
insert into time(
  {','.join(time_cols)}) values
  ({('%s,'*(len(time_cols)-1)) + '%s'} )
  
ON CONFLICT (start_time) 
DO NOTHING
"""
)

# FIND SONGS

song_select = ("""
select songs.song_id, artists.artist_id
from songs
join artists
  on songs.artist_id = artists.artist_id 
where songs.title=%s 
  and artists.name = %s
  and songs.duration = %s
""")

# QUERY LISTS

create_table_queries = [songplay_table_create,
                        user_table_create,
                        song_table_create,
                        artist_table_create,
                        time_table_create]
drop_table_queries = [songplay_table_drop,
                      user_table_drop,
                      song_table_drop,
                      artist_table_drop,
                      time_table_drop]