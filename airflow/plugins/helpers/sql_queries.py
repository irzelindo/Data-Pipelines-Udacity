class SqlQueries:
    
    staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
    staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
    songplay_table_drop = "DROP TABLE IF EXISTS songplays"
    user_table_drop = "DROP TABLE IF EXISTS users"
    song_table_drop = "DROP TABLE IF EXISTS songs"
    artist_table_drop = "DROP TABLE IF EXISTS artists"
    time_table_drop = "DROP TABLE IF EXISTS time"
    
    artist_table_create =('''
        CREATE TABLE IF NOT EXISTS artists (
            artistid varchar(256),
            name varchar(256),
            location varchar(256),
            latitude numeric(18,0),
            longitude numeric(18,0)
        );
    ''')
    
    songplay_table_create = ('''
        CREATE TABLE IF NOT EXISTS songplays (
            playid int IDENTITY(0,1) PRIMARY KEY,
            start_time timestamp,
            userid int4,
            "level" varchar(256),
            songid varchar(256),
            artistid varchar(256),
            sessionid int4,
            location varchar(256),
            user_agent varchar(256)
        );
    ''')
    
    song_table_create=('''
        CREATE TABLE IF NOT EXISTS songs (
            songid varchar(256),
            title varchar(256),
            artistid varchar(256),
            "year" int4,
            duration numeric(18,0),
            CONSTRAINT songs_pkey PRIMARY KEY (songid)
        );
    ''')
    
    staging_events_table_create=('''
        CREATE TABLE IF NOT EXISTS staging_events (
            artist varchar(256),
            auth varchar(256),
            firstName varchar(256),
            gender varchar(256),
            itemInSession int4,
            lastName varchar(256),
            length numeric(18,0),
            "level" varchar(256),
            location varchar(256),
            "method" varchar(256),
            page varchar(256),
            registration numeric(18,0),
            sessionId int4,
            song varchar(256),
            status int4,
            ts int8,
            userAgent varchar(256),
            userId int4
        );
    ''')
    
    staging_songs_table_create=('''
        CREATE TABLE IF NOT EXISTS staging_songs (
            artist_id varchar(256),
            artist_name varchar(256),
            artist_latitude numeric(18,0),
            artist_longitude numeric(18,0),
            artist_location varchar(256),
            song_id varchar(256),
            title varchar(256),
            duration numeric(18,0),
            num_songs int4,
            "year" int4
        );
    ''')
    
    time_table_create=('''
        CREATE TABLE IF NOT EXISTS time (
            start_time timestamp NOT NULL,
            "hour" int4,
            "day" int4,
            week int4,
            "month" varchar(256),
            "year" int4,
            weekday varchar(256),
            CONSTRAINT time_table_pkey PRIMARY KEY (start_time)
        );
    ''')
    
    user_table_create=('''
        CREATE TABLE IF NOT EXISTS users (
            userid int4 NOT NULL,
            first_name varchar(256),
            last_name varchar(256),
            gender varchar(256),
            "level" varchar(256),
            CONSTRAINT users_pkey PRIMARY KEY (userid)
        );
    ''')

    songplay_table_insert = ("""
        (start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        SELECT
        events.start_time, 
        events.userid, 
        events.level, 
        songs.song_id, 
        songs.artist_id, 
        events.sessionid, 
        events.location, 
        events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
        FROM staging_events
        WHERE page='NextSong') events
        LEFT JOIN staging_songs songs
        ON events.song = songs.title
        AND events.artist = songs.artist_name
        AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs 
        WHERE song_id IS NOT NULL
        AND artist_id IS NOT NULL
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
        WHERE artist_id IS NOT NULL
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)
    
    create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create,artist_table_create,time_table_create]
    
    drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
