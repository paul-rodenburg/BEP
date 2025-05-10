# Paul Rodenburg - BEP

## Download data

<div style="
  border: 1px solid rgba(255, 255, 255, 0.3);
  background: rgba(255, 0, 25, 0.2);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  text-align: center;
  padding: 10px;
  border-radius: 10px;
  color: white;
">
  <strong>⚠️ Warning:</strong> Make sure to have at least 1.5TB free space as the files are really big.
</div>
<br>

1. Download a program that can download torrents (for example [Transmission](https://transmissionbt.com/))
2. Go to [Reddit comments/submissions 2025-01](https://academictorrents.com/details/4fd14d4c3d792e0b1c5cf6b1d9516c48ba6c4a24) and download .torrent file
3. Go to [Reddit subreddits metadata, rules and wikis 2025-01](https://academictorrents.com/details/5d0bf258a025a5b802572ddc29cde89bf093185c) and download the .torrent file
4. Open up the installed Transmission application from step 1 and import the .torrent files. This will start both downloads
5. The downloaded files are compressed .zst files, to uncompress these files use an application like [7-zip](https://www.7-zip.org/)
6. Move the uncompressed files to the `data` folder and specify its locations in `config.json` (this already contains default locations, so if data files are placed in the same destinations then no changes are needed)

**Note that files having data from other months can also be used, just make sure config.json has the right path to them then**

## Steps to make the Reddit databases

<div style="
  border: 1px solid rgba(255, 255, 255, 0.3);
  background: rgba(255, 0, 25, 0.2);
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  text-align: center;
  padding: 10px;
  border-radius: 10px;
  color: white;
">
  <strong>⚠️ Warning:</strong> Make sure that you have downloaded all the datafiles mentioned above,
  <br> and all the paths in <code>config.json</code> point to those files.
</div>
<br>

1. Install the database drivers:
   - MySQL [Download MySQL drivers](https://dev.mysql.com/downloads/installer/)
     - For macOS: first install [HomeBrew](https://brew.sh/) and then run `brew install mysql` in the terminal
   - PostgreSQL [Download PostgreSQL drivers](https://www.postgresql.org/download/)
   - MongoDB [Download MongoDB drivers](https://www.mongodb.com/docs/manual/administration/install-community/)
   - No driver installation for sqlite is necessary
2. <i>(Optional)</i> Run `line_counts.py`, this will create a JSON file consisting of the number of lines for each datafile. This is then used for the progress bars to give you an estimation of the running time. When you choose to not run this script, it will cache the datafile line counts automatically when needed. <br><strong>But note that you then have to wait sometimes before the execution of code can continue.</strong>
3. Run `count_characters_db.py`, this will create a JSON file which contains the maximum character count per attribute in each datafile. This is then used to determine for MySQL whether is has to use `TEXT` or `LONGTEXT` for attributes. (Simply setting `LONGTEXT` for all attributes negatively impacts performance)
4. Run the following files in the folder `data_to_db` to make the databases
   - `make_mysql_database.py`
   - `make_postgresql_database.py`
   - `make_mongdb_database.py`
   - `make_sqlite_database.py`
5. Run the following files in the `metrics` folder to test the performance of the different databases:
   - `creation_time_db_metric.py`: saves the creation time of the databases to `creation_time.xlsx`
   - `disk_space_db_metric.py`: saves the disk usage per databases to `disk_usage.xlsx`
   - `query_mongodb_metrics.py`: tests queries for the MongoDB database. Results saved to `output` folder
   - `query_sql_metrics.py`: tests queries for the SQL databases. Results saved to `output` folder

## ER Diagram

<details>
  <summary>Click to view Relational Diagram (collapsed due to image size)</summary>

![Relational Diagram](images/Relational%20diagram.svg)

</details>

## Sources

- `ignored.txt`: [Download ignored.txt here](https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/ignored.txt)
  - Contains usernames that are ignored (common bot and moderator usernames and deleted user flag name)
