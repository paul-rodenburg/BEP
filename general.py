import orjson as json
import sqlite3
from typing import Any, Mapping
from pymongo import MongoClient
from collections import deque
from pymongo.synchronous.database import Database
from sqlalchemy import Engine, text, create_engine
import subprocess
import os
from classes.DBType import DBTypes, DBType
from datetime import datetime
import psycopg2
from sqlalchemy import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def load_json(file_path, make_file_if_not_exists=True) -> dict | list:
    """
    Loads the content of a JSON file.

    :param file_path: Path to the JSON file
    :param make_file_if_not_exists: if True, creates the file if it doesn't exist

    :return: A dict with the content of the JSON file
    """
    if not os.path.isfile(file_path) and not make_file_if_not_exists:
        raise FileNotFoundError(f"File {file_path} does not exist. You can set the parameter 'make_file_if_not_exists' to True to create the file.")

    if not os.path.isfile(file_path):
        with open(file_path, 'wb') as f:
            f.write(json.dumps({}))
        return {}
    with open(file_path, "r", encoding='utf-8') as f:
        content = f.read()
        if not content.strip():
            return {}
        return json.loads(content)


# internal cache
_json_cache = {}


def load_json_cached(file_path, make_file_if_not_exists=True) -> dict | list:
    """
    Loads JSON from a file, with in-memory caching.
    If the file was already loaded, returns the cached version.

    :param file_path: path to the JSON file
    :param make_file_if_not_exists: if True, creates the file if it doesn't exist
    """
    if file_path not in _json_cache:
        _json_cache[file_path] = load_json(file_path, make_file_if_not_exists)
    return _json_cache[file_path]


def write_json(data: dict | list, file_path: str):
    """
    Writes the content of a dict or list to a json file.

    :param data: the data to write to the json file
    :param file_path: path to the json file to write to
    """
    with open(file_path, "wb") as f:
        f.write(json.dumps(data, option=json.OPT_INDENT_2))


def extract_line(line_nr, content_file_path) -> str | None:
    """"
    Extracts a line from a file given a line number.

    :param line_nr: The line number to extract.
    :return: The extracted line, or None if the line number is invalid or the file is empty.
    """
    line_nr = int(line_nr)
    count_line_find = 0
    with open(content_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            count_line_find += 1
            if count_line_find == line_nr:
                return line
    return None


def get_tables_database(engine: Engine, db_type: DBType):
    """
    Gets the tables of a database.

    :param engine: connection to the database

    :raises ValueError: if the connection type is not supported
    """
    match db_type.get_type():
        case DBTypes.SQLITE:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
            tables = result.fetchall()
            return [table[0] for table in tables]
        case DBTypes.MYSQL:
            with engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
            return [row[0] for row in result.fetchall()]
        case DBTypes.POSTGRESQL:
            with engine.connect() as conn:
                result = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public';"))
            return [row[0] for row in result.fetchall()]
        case DBTypes.MONGODB:
            db = make_mongodb_client()
            collections = list(db.list_collection_names())
            db.client.close()
            return collections
        case _:
            raise ValueError(f'Unknown database type: {db_type}')


def read_file_reverse(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        # Initialize a deque with a fixed size, so it can store lines in reverse order
        lines = deque(maxlen=1)  # Using deque to simulate the reverse reading
        f.seek(0, 2)  # Seek to the end of the file
        buffer = ''

        while f.tell() > 0:
            f.seek(f.tell() - 1024, 0)  # Move the pointer back 1024 bytes
            buffer = f.read(1024) + buffer  # Read the chunk and add to buffer
            lines = deque(buffer.splitlines(), maxlen=1)
            if lines:
                for line in reversed(lines):
                    yield line  # Yield lines in reverse order


def check_files(db_type: None | DBType = None):
    # Check if character count file exists
    # This is necessary to change TEXT to LONGTEXT for some attributes in MySQL, because of long lengths of data
    if db_type and db_type.is_type(DBTypes.MYSQL) and not os.path.isfile('character_lengths.json'):
        print("character_lengths.json not found. Running the script now...")
        subprocess.run(["python", "count_characters_db.py"])
        # raise FileNotFoundError(
        #     "character_lengths.json not found. Please run the script 'count_characters_db.py' first.")

    data = load_json('config.json')
    files = list(data['data_files_tables'].keys())
    files.append('character_lengths.json')

    files_not_found = []
    for file in files:
        if not os.path.isfile(file):
            files_not_found.append(file)

    if files_not_found:
        file_not_found_text = "\n".join(files_not_found)
        raise FileNotFoundError(f"The following files were not found:\n{file_not_found_text}")


def get_count_rows_database(conn, table_name):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    conn.close()
    return count


def make_sqlite_engine(db_type: DBType):
    """
    Makes a sqlite connection
    :return: a sqlite connection
    """
    data = load_json('config.json')['sqlite']
    db_folder = data['db_folder']
    engine = create_engine(f'sqlite:///{db_folder}/reddit_data_{db_type.name_suffix}.db')
    return engine


def make_postgres_engine(db_type: DBType = None):
    """
    Ensures the PostgreSQL database exists and returns a SQLAlchemy engine.

    :param db_type: Database type to connect to.

    :return: SQLAlchemy engine for the PostgreSQL database.
    """
    data = load_json('config.json')['postgresql']
    host = data["host"]
    user = data["username"]
    password = data["password"]
    port = data["port"]
    if db_type:
        db_name = f"reddit_data_{db_type.name_suffix}"
    else:
        db_name = data["db_name"]
    custom_engine_url = data.get("custom_engine_url")

    # If a custom engine URL is provided, use it directly
    if custom_engine_url is not None:
        engine = create_engine(custom_engine_url)
        return engine

    # Connect to the default 'postgres' database to check/create the target DB
    conn = psycopg2.connect(
        dbname="postgres",
        user=user,
        password=password,
        host=host,
        port=port
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Check if DB exists, if not, create it
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone()
    if not exists:
        cur.execute(f'CREATE DATABASE "{db_name}"')
        print(f"[PostgreSQL] Database '{db_name}' created.")

    cur.close()
    conn.close()

    # Return SQLAlchemy engine for the created or existing database
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    return engine


def make_mysql_engine(db_type: DBType|None=None):
    """
    Makes a sqlite connection
    :return: a sqlite connection
    """
    data = load_json('config.json')['mysql']
    host = data["host"]
    user = data["username"]
    password = data["password"]
    custom_engine_url = data["custom_engine_url"]
    if db_type:
        db_name = db_type.name_suffix
    else:
        db_name = data["db_name"]

    if custom_engine_url is not None:
        engine_url = custom_engine_url
    elif password is not None:
        engine_url = f"mysql+pymysql://{user}:{password}@{host}"
    else:
        engine_url = f"mysql+pymysql://{user}@{host}"

    if db_type is not None:
        engine_url = f"{engine_url}/reddit_data_{db_name}"

    engine = create_engine(engine_url)
    return engine


def make_mongodb_client(db_type: DBType=None) -> Database[Mapping[str, Any] | Any]:
    """
    Makes a mongodb connection

    :param db_type: Database type to connect to.
    :return: A mongodb connection
    """
    data = load_json('config.json')['mongodb']
    host = data["host"]
    port = data["port"]
    db_name = f"reddit_data_{db_type.name_suffix}"

    custom_engine_url = data["custom_engine_url"]
    # Connect to MongoDB
    if custom_engine_url is not None:
        client = MongoClient(custom_engine_url)
    else:
        client = MongoClient(f"mongodb://{host}:{port}/")

    # Access a database (this creates it if it doesn't exist)
    db = client[db_name]

    return db

def update_summary_log(db_type: DBType, data_file: str, start_time: datetime, end_time: datetime, line_count: int, total_lines: int, tables: list|None, chunk_size: int, sql_writes: int|None):
    """
    Updates the summary log file.

    :param db_type: database type
    :param data_file: data file name
    :param start_time: progress bar string
    :param end_time: progress bar string
    :param line_count: number of lines processed
    :param total_lines: total number of lines in the data file
    :param tables: list of tables processed
    :param chunk_size: number of lines written to the sql database at a time
    :param sql_writes: number of sql writes
    """
    summary_path = f"logs/summaries/summary_{db_type.to_string()}_{db_type.name_suffix}.json"
    current_summary = load_json(summary_path)

    begin_time_formatted = start_time.strftime("%d %B %Y %H:%M.%S")
    end_time_formatted = end_time.strftime("%d %B %Y %H:%M.%S")
    time_elapsed_seconds = int(end_time.timestamp()) - int(start_time.timestamp())
    if not isinstance(tables, list):
        tables = [tables]

    info_to_add_log = {'start_time': int(start_time.timestamp()), 'end_time': int(end_time.timestamp()),
                       'start_time_formatted': begin_time_formatted, 'end_time_formatted': end_time_formatted,
                       'time_elapsed_seconds': time_elapsed_seconds, 'tables': tables,
                       'line_count': line_count, 'chunk_size': chunk_size, 'total_lines': total_lines,
                       'sql_writes': sql_writes}
    if db_type.is_type(DBTypes.MONGODB):
        del info_to_add_log['tables']
        del info_to_add_log['sql_writes']
    current_summary[data_file] = info_to_add_log
    write_json(current_summary, summary_path)


def should_skip(line: dict|list[dict], primary_keys: list) -> bool:
    """
    Determines whether the line should be skipped based on the primary keys and other values.
    If any primary key value is None, the line is skipped. If all other values are None, the line is also skipped.

    :param line: Line to check.
    :param primary_keys: Primary keys to check.

    :return: True if the line should be skipped, False otherwise.

    """

    # Check if any primary key value is None or empty
    if isinstance(line, list):
        line = line[0]
    if any(line.get(key) is None or line.get(key) == '' for key in primary_keys):
        return True

    # Get all other keys not in primary_keys
    other_keys = [k for k in line.keys() if k not in primary_keys]

    # If all other values are None or empty
    if all(line.get(k) is None or line.get(k) == '' for k in other_keys):
        return True
    return False

