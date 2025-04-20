import json
import sqlite3
import re
from collections import deque

from sqlalchemy import Engine, text, create_engine

import os

def extract_line(line_nr, content_file_path):
    line_nr = int(line_nr)
    count_line_find = 0
    with open(content_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            count_line_find += 1
            if count_line_find == line_nr:
                return line


def get_primary_key(table_name, schema_json_file="schemas/db_schema.json"):
    """
    Gets primary key columns of a table.

    :param table_name: Name of the table.
    :param schema_json_file: Path to the schema json file.

    :return: List of primary key columns.
    """

    with open(schema_json_file, "r", encoding="utf-8") as f:
        schema = json.load(f)

    primary_keys = schema[table_name]['primary_keys']

    return primary_keys


def get_database_type(conn) -> str:
    """
    Gets the database type based on the connection.

    :param conn: connection to the database
    :raises ValueError: if the connection type is not supported
    :return: the database type ('postgresql' or 'mysql' or 'sqlite')
    """
    if isinstance(conn, sqlite3.Connection):  # SQLite
        return 'sqlite'

    elif isinstance(conn, Engine):  # PostgreSQL or MySQL
        db_type = conn.dialect.name  # Detect database type
        match db_type:
            case 'postgresql':
                return 'postgresql'
            case 'mysql':
                return 'mysql'
            case 'sqlite':
                return 'sqlite'
            case _:
                raise ValueError(f"Only SQLite, PostgreSQL, and MySQL connections are supported, not {db_type}")

    else:
        raise ValueError(f"Only SQLite, PostgreSQL, and MySQL connections are supported, not {type(conn)}")

def get_tables_database(engine):
    """
    Gets the tables of a database.

    :param engine: connection to the database

    :raises ValueError: if the connection type is not supported
    """
    db_type = get_database_type(engine)
    match db_type:
        case 'sqlite':
            with engine.connect() as conn:
                result = conn.execute(text("SELECT name FROM sqlite_master WHERE type='table';"))
            tables = result.fetchall()
            return [table[0] for table in tables]
        case 'mysql':
            with engine.connect() as conn:
                result = conn.execute(text("SHOW TABLES"))
            return [row[0] for row in result.fetchall()]
        case 'postgresql':
            with engine.connect() as conn:
                result = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public';"))
            return [row[0] for row in result.fetchall()]
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



def check_files():
    with open('config.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
        files = list(data['data_files_tables'].keys())

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

def make_sqlite_engine():
    """
    Makes a sqlite connection
    :return: a sqlite connection
    """
    with open('config.json', 'r', encoding='utf-8') as f:
        data = json.load(f)['sqlite']
        db_location_relative = data['db_location_relative']
    engine = create_engine(f'sqlite:///{db_location_relative}')
    return engine

def make_postgres_engine():
    """
    Makes a sqlite connection
    :return: a sqlite connection
    """
    with open('config.json', 'r', encoding='utf-8') as f:
        data = json.load(f)['postgresql']
        host = data["host"]
        user = data["username"]
        password = data["password"]
        port = data["port"]
        db_name = data["db_name"]
        custom_engine_url = data["custom_engine_url"]
    if custom_engine_url is not None:
        engine = create_engine(custom_engine_url)
    else:
        engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db_name}")
    return engine


def make_mysql_engine(db_name=None):
    """
    Makes a sqlite connection
    :return: a sqlite connection
    """
    with open('config.json', 'r', encoding='utf-8') as f:
        data = json.load(f)['mysql']
        host = data["host"]
        user = data["username"]
        password = data["password"]
        custom_engine_url = data["custom_engine_url"]

    if custom_engine_url is not None:
        engine_url = custom_engine_url
    elif password is not None:
        engine_url = f"mysql+pymysql://{user}:{password}@{host}"
    else:
        engine_url = f"mysql+pymysql://{user}@{host}"

    if db_name is not None:
        engine_url = f"{engine_url}/{db_name}"

    engine = create_engine(engine_url)
    return engine

def make_mongodb_engine():
    """
    Makes a sqlite connection
    :return: a sqlite connection
    """
    raise ValueError(f"'make_mongodb_engine' NEEDS TO BE IMPLEMENTED")
    with open('config.json', 'r', encoding='utf-8') as f:
        data = json.load(f)['sqlite']
        db_location_relative = data['db_location_relative']
    engine = create_engine(f'sqlite:///{db_location_relative}')
    return engine
