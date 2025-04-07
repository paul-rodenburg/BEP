# Paul Rodenburg - BEP

## Download data

### (WARNING: Make sure to have at least 1.2TB free space as the files are really big)

1. Download a program that can download torrents (for example [Transmission](https://transmissionbt.com/))
2. Go to [Reddit comments/submissions 2025-01](https://academictorrents.com/details/4fd14d4c3d792e0b1c5cf6b1d9516c48ba6c4a24) and download .torrent file
3. Go to [Reddit subreddits metadata, rules and wikis 2025-01](https://academictorrents.com/details/5d0bf258a025a5b802572ddc29cde89bf093185c) and download the .torrent file
4. Open up the installed Transmission application from step 1 and import the .torrent files. This will start both downloads
5. The downloaded files are compressed .zst files, to uncompress these file use an application like [7-zip](https://www.7-zip.org/)
6. Move the uncompressed files to the `data` folder and specify its locations in `config.py`


## Steps to make database(s)

1. Install the database drivers:
   - MySQL [Download MySQL drivers](https://dev.mysql.com/downloads/installer/)
     - For macOS: first install [HomeBrew](https://brew.sh/) and then run `brew install mysql` in the terminal
   - PostgreSQL [Download PostgreSQL drivers](https://www.postgresql.org/download/)
   - MongoDB [Download MongoDB drivers](https://www.mongodb.com/docs/manual/administration/install-community/)
   - No driver installation for sqlite is necessary
2. Run the following files in the folder `data_to_db` to make the databases
   - `make_mysql_database.py`
   - `make_postgresql_database.py`
   - `make_mongdb_database.py`
   - `make_sqlite_database.py`

[other steps will be added later]

## Sources

- `ignored.txt`: [Download ignored.txt here](https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/ignored.txt)
  - Contains usernames that are ignored (common bot and moderator usernames and deleted user flag name)
