CREATE DATABASE spotify_db;

CREATE OR REPLACE storage integration s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '${AWS_ROLE_ARN}'
    STORAGE_ALLOWED_LOCATIONS = ('${S3_BUCKET_LOCATION}')
    COMMENT = 'Creating connection to S3'

DESC integration s3_init;

// Create file format object
CREATE OR REPLACE file format csv_fileformat
    TYPE = csv
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL','null')
    EMPTY_FIELD_AS_NULL = TRUE;

// Create stage
CREATE OR REPLACE stage spotify_stage
    URL = '${S3_STAGE_URL}'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = csv_fileformat;

LIST @spotify_stage;

CREATE OR REPLACE TABLE tbl_album(
    album_id STRING,
    name STRING,
    release_date DATE,
    total_tracks INT,
    url STRING
);

CREATE OR REPLACE TABLE tbl_artists(
    artist_id STRING,
    name STRING,
    url STRING
);

CREATE OR REPLACE TABLE tbl_songs(
    song_id STRING,
    song_name STRING,
    duration_ms INT,
    url STRING,
    popularity INT,
    song_added DATE,
    album_id STRING,
    artist_id STRING
);

SELECT * FROM tbl_songs;


// Create snowpipe
CREATE OR REPLACE SCHEMA pipe;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_album_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_album
FROM @spotify_db.public.spotify_stage/album/;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_artists_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_artists
FROM @spotify_db.public.spotify_stage/artist/;

CREATE OR REPLACE pipe spotify_db.pipe.tbl_songs_pipe
auto_ingest = TRUE
AS
COPY INTO spotify_db.public.tbl_songs
FROM @spotify_db.public.spotify_stage/songs/;

DESC pipe pipe.tbl_album_pipe;

DESC pipe pipe.tbl_artists_pipe;

DESC pipe pipe.tbl_songs_pipe;


