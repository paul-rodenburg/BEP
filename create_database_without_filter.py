from datetime import datetime
import pandas as pd
import json
import os
import time
import math
from itertools import chain
from sqlalchemy.engine import Engine
from sqlalchemy import text
from tqdm import tqdm
from general import get_primary_key
from line_counts import get_line_count_file
from config import *
import sqlite3
import re

progress_bar = None
clean_errors = 0

def unnest(lst):
    """
    Unnests a list, however if a list is not nested it will not unnest it to a list of single characters.

    :param lst: list to unnest

    :return: unnested list
    """
    if all(not isinstance(i, list) for i in lst):
        return lst  # Return as-is if it's already flat
    return list(chain.from_iterable(lst))


def get_table_columns(sql_file_path, table_name) -> list:
    """
    Gets the table columns from a sqlite database file.

    :param sql_file_path: Path to the sqlite database file.
    :param table_name: Name of the table to extract columns from.

    :return: list of table names in the sqlite database file.
    """
    with open(sql_file_path, "r", encoding="utf-8") as f:
        sql = f.read()

    # Regular expression to extract the table definition
    table_pattern = rf"CREATE TABLE `{table_name}` \((.*?)\);"
    match = re.search(table_pattern, sql, re.S)

    if not match:
        return []

    # Extracting column definitions
    columns_block = match.group(1)

    # Regular expression to extract column names
    column_pattern = r"`(\w+)`"
    columns = re.findall(column_pattern, columns_block)

    return columns

def clean_line(line_input, tables, table_columns, ignored_author_names) -> dict[str, dict]:
    """
    Gets a line and cleans it for all the tables.

    :param line_input: line to clean
    :param tables: tables for the line
    :param table_columns: columns for the tables
    :param ignored_author_names: author names to ignore.
    :return: A dict with as key the table name and value the cleaned line for that table
    """
    global clean_errors

    try:
        line_input = json.loads(line_input)
    except:
        clean_errors += 1
        return None

    cleaned_data = dict()
    for table in tables:
        cleaned_data[table] = None

    for table in tables:
        line = line_input
        items_to_keep = table_columns[table]
        if table in 'banned':
            if line['banned_at_utc'] is None and line['banned_by'] is None:
                continue
        if table == 'removed':
            if line['removal_reason'] is None and line['removed_by'] is None:
                continue
        if table == 'wiki':
            try:
                dt = datetime.fromisoformat(line['revision_date'].replace("Z", "+00:00"))
            except Exception as e:
                print(f'KEY ERROR! REVISION DATE: {e}')
                print(line)
                exit(1)
            epoch_time = int(dt.timestamp())
            line['revision_date'] = epoch_time
            pattern = r"/r/([^/]+)"
            subreddit_match = re.search(pattern, line['path'])
            if not subreddit_match:  # If the subreddit could not be found then return None since this data will be useless
                continue
            line['subreddit'] = subreddit_match.group(1)

        if table == 'post' or table == 'comment':  # To not get any postgreSQL errors
            if int(line['edited']) == 0:
                line['edited'] = False
            else:
                line['edited'] = True

        if table == 'author':
            if line['author'].strip().lower() in ignored_author_names:
                continue

        cleaned_line = {key: line[key] for key in items_to_keep if key in line}
        cleaned_data[table] = cleaned_line

    return cleaned_data

def process_cleaned_lines(cleaned_lines_dct) -> dict[str, pd.DataFrame]:
    """
    Puts the list of cleaned lines into a dataframe, which is then put into the value of a dictionary with as key the table name.

    :param cleaned_lines_dct: cleaned lines dictionary

    :return: A dict with as key the table name and value the cleaned lines in a pandas DataFrame.
    """
    for table_name, data in cleaned_lines_dct.items():
        if data is None:
            continue

        cleaned_lines_dct[table_name] = pd.DataFrame(data)
        if len(cleaned_lines_dct[table_name]) == 0 or cleaned_lines_dct[table_name] is None:
            cleaned_lines_dct[table_name] = None
            continue

        primary_key_column = get_primary_key(table_name)
        columns = cleaned_lines_dct[table_name].columns.tolist()
        for pm in primary_key_column:
            if pm not in columns:
                cleaned_lines_dct[table_name] = None
                continue
        if cleaned_lines_dct[table_name] is not None and not cleaned_lines_dct[table_name].empty:
            cleaned_lines_dct[table_name] = cleaned_lines_dct[table_name].drop_duplicates(subset=primary_key_column)
            cleaned_lines_dct[table_name] = cleaned_lines_dct[table_name].map(lambda x: str(x) if isinstance(x, (list, dict)) else x)
            cleaned_lines_dct[table_name] = cleaned_lines_dct[table_name].map(lambda x: x.replace("\x00", "") if isinstance(x, str) else x)

    return cleaned_lines_dct


sql_count = 0

def process_table(data_file, tables, conn, table_columns, ignored_author_names, chunk_size=10_000):
    """
    Processes tables, so writing the data to a database.

    :param data_file: path to data file
    :param tables: tables to process
    :param conn: database connection
    :param table_columns: dictionary containing tables names as keys and the value are the column names corresponding to the tables
    :param ignored_author_names: author names to ignore.
    :param chunk_size: number of lines to read at a time
    """
    global sql_count

    added_count = 0
    for chunk_data in extract_lines(data_file, tables, table_columns, ignored_author_names, chunk_size):
        for table_name, data in chunk_data.items():
            if data is not None and not data.empty:
                write_to_db(data, table_name, conn, len(chunk_data), chunk_size=chunk_size)
                added_count += 1
    sql_count = 0  # Reset count for the progress bar
    if added_count == 0:
        print(f'Error! All chunks of {tables} were empty')


def extract_lines(data_file, tables, table_columns, ignored_author_names, chunk_size=10_000) -> dict[str, pd.DataFrame]:
    """
    Processes lines in the from the Reddit data file.

    :param data_file: path to the Reddit data file
    :param tables: tables to process
    :param table_columns: dictionary containing tables names as keys and the value are the column names corresponding to the tables
    :param ignored_author_names: author names to ignore
    :param chunk_size: number of lines to read at a time

    :return: A dict with as key the table name and value the cleaned lines for that table in pandas DataFrame
    """
    global progress_bar
    lines_clean = {}
    for table_name in tables:
        lines_clean[table_name] = []


    progress_bar_total = min(get_line_count_file(data_file), LINES_SUBSET)
    progress_bar = tqdm(total=progress_bar_total, desc=f"Processing {len(tables)} table(s): {tables} (from {data_file.split('/')[-1]})")

    lines_cleaned_count = 0
    with open(data_file, 'r', encoding='utf-8') as f_data:

        for line in f_data:
            cleaned_data = clean_line(line, tables, table_columns, ignored_author_names)
            for table_name, line_cleaned in cleaned_data.items():
                if line_cleaned is not None:
                    lines_clean[table_name].append(line_cleaned)

            lines_cleaned_count += 1
            progress_bar.update(1)

            if lines_cleaned_count % chunk_size == 0 and lines_cleaned_count > 0:
                yield process_cleaned_lines(lines_clean)
                # Clean the dict for the next iteration
                lines_clean = dict()
                for table_name in tables:
                    lines_clean[table_name] = []
            if lines_cleaned_count >= LINES_SUBSET:
                break

    progress_bar.close()
    if lines_clean:
        yield process_cleaned_lines(lines_clean)


def write_to_db(df, table, conn, len_tables, chunk_size=10_000):
    """
    Write dataframe to database.

    :param df: pandas DataFrame
    :param table: table name
    :param conn: database connection
    :param len_tables: number of tables that are currently being processed (only used to display in tqdm progress bar)
    :param chunk_size: number of lines to read at a time
    """
    global sql_count, progress_bar
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=5000)
    sql_count += 1
    progress_bar.set_postfix_str(f'[{sql_count:,}/{math.ceil(progress_bar.total / chunk_size * len_tables):,} SQL writes]')


def is_file_tables_added_db(data_file, tables, db_info_file) -> list:
    """
    Gets the table names of the table names that are not (fully) processed, so not (competely) added to the database.

    :param data_file: path to the reddit data file
    :param tables: list of table names to check
    :param db_info_file: path to the database info file

    :return: list of table names that need to be added to the database
    """
    if os.path.isfile(db_info_file):
        with open(db_info_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        for obj in data:
            if obj['file'] == data_file:
                # Find tables that are in `tables` but not in `success_tables`
                missing_tables = list(set(tables) - set(obj['success_tables']))
                return missing_tables

    # If file doesn't exist or file not found, return all tables
    return tables


def add_file_table_db_info(data_file, tables, db_info_file):
    """
    Adds content to the success_tables in the database specific json file.

    :param data_file: path to the reddit data file
    :param tables: table names to add to the success_tables field in the db_info_file
    :param db_info_file: path to the json db info file
    """
    if not os.path.isfile(db_info_file):
        with open(db_info_file, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4)

    with open(db_info_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    file_entry = next((obj for obj in data if obj['file'] == data_file), None)

    if file_entry:
        if not all(item in file_entry['success_tables'] for item in tables):
            file_entry['success_tables'].append(tables)
            file_entry['success_tables'] = unnest(file_entry['success_tables'])
            file_entry['success_tables'] = list(set(file_entry['success_tables']))
    else:
        data.append({'file': data_file, 'success_tables': tables})

    with open(db_info_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)

def get_data_file(data_files_tables, table_name) -> str|None:
    """
    Gets the data file path corresponding to the given table name.

    :param data_files_tables: dictionary that maps data file paths to table names
    :param table_name: the name of the table corresponding to a data file.

    :return: path to the data file if table name found, otherwise None
    """
    for file, tables in data_files_tables.items():
        if table_name in tables:
            return file
    return None  # Returns None if the table_name is not found

def load_json(file_path) -> dict:
    """
    Loads the content of a json file.

    :param file_path: path to the json file

    :return: a dict with the content of the json file
    """
    with open(file_path, "r") as f:
        return json.load(f)


def get_tables_to_skip(json_data) -> set:
    """
    Gets the table names of tables that already do not include any duplicates

    :param json_data: data of the database specific info
    """
    tables_to_skip = set()
    for entry in json_data:
        if "duplicates_removed" in entry:
            tables_to_skip.update(entry["duplicates_removed"])
    return tables_to_skip

def update_json_with_table_duplicates(json_file, table_name, data_file):
    """
    Adds the table names of tables where duplicates are removed to the database specific info json file.

    :param json_file: path to json file with database specific info
    :param table_name: table name of table where duplicates are removed
    :param data_file: path to the file with reddit data

    """
    json_data = load_json(json_file)
    for entry in json_data:
        if "success_tables" in entry and table_name in entry["success_tables"] and data_file in entry["file"]:
            if "duplicates_removed" not in entry:
                entry["duplicates_removed"] = []
            if table_name not in entry["duplicates_removed"]:
                entry["duplicates_removed"].append(table_name)
    with open(json_file, "w") as f:
        json.dump(json_data, f, indent=2)


def remove_duplicates_db(conn, json_file):
    """
    Removes duplicate tables from a database.

    :param conn: connection to database
    :param json_file: path to json file with database specific info
    """
    print("\033[91mREMOVING DUPLICATES TAKES TOO MUCH RAM! NEEDS BETTER IMPLEMENTATION\033[0m")
    return
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [table[0] for table in cursor.fetchall()]

    json_data = load_json(json_file)
    tables_to_skip = get_tables_to_skip(json_data)
    tables_to_skip.update(['comment', 'post'])  # There will never be duplicates in post and comment tables

    progress_bar_duplicates_removed = tqdm(total=len(set(tables) - tables_to_skip), desc='Removing duplicates from database')
    for table_name in tables:
        data_file = get_data_file(data_files_tables, table_name)
        if table_name in tables_to_skip:
            continue
        progress_bar_duplicates_removed.update()
        progress_bar_duplicates_removed.set_postfix_str(f"Currently at '{table_name}'")
        q = f'SELECT * FROM {table_name}'
        df_clean = pd.read_sql_query(q, conn)

        try:
            df_clean = df_clean.drop_duplicates(subset=get_primary_key(table_name))
        except Exception as e:
            print(f'Error with table {table_name} and primary key {get_primary_key(table_name)}: {e}')
            continue

        df_clean.to_sql(table_name, conn, if_exists='replace', index=False)
        df_clean = pd.DataFrame()
        update_json_with_table_duplicates(json_file, table_name, data_file)

def clean_json_duplicates(json_file):
    """
    Cleans the json duplicates field, ensures that a table name is only in the duplicates_removed field if in success_tables.

    :param json_file: the path to the json with database specific info
    """
    json_data = load_json(json_file)
    for entry in json_data:
        if "duplicates_removed" in entry and "success_tables" in entry:
            entry["duplicates_removed"] = [table for table in entry["duplicates_removed"] if table in entry["success_tables"]]
    with open(json_file, "w") as f:
        json.dump(json_data, f, indent=2)


def delete_table_db(table_name, engine):
    """
    Deletes a table from the database.

    :param table_name: name of the table to delete
    :param engine: connection to the database

    :raises ValueError: if the connection type is not supported
    """
    delete_confirm = input(f'Deleting table {table_name}... Sure? (y/n)')
    if delete_confirm.lower().strip() == 'y':
        pass
    elif delete_confirm.lower().strip() == 'n':
        return
    else:
        print(f'{delete_confirm} is not a valid option. Please try again.')
        delete_table_db(table_name, engine)
        return

    if isinstance(engine, sqlite3.Connection):  # SQLite connection
        engine.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f'Deleted table {table_name} in SQLITE database')

    elif isinstance(engine, Engine):  # PostgreSQL or MySQL connection
        db_type = engine.dialect.name  # Detect the database type

        if db_type == "postgresql":  # PostgreSQL
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            print(f'Deleted table {table_name} in PostgreSQL database')

        elif db_type == "mysql":  # MySQL
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f'Deleted table {table_name} in MySQL database')

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    else:
        raise ValueError(f'{type(engine)} is not supported')


def get_tables(engine):
    """
    Gets the tables of a database.

    :param engine: connection to the database

    :raises ValueError: if the connection type is not supported
    """
    # Establish a connection from the SQLAlchemy engine
    if isinstance(engine, sqlite3.Connection):  # SQLite
        result = engine.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = result.fetchall()
        return [table[0] for table in tables]

    elif isinstance(engine, Engine):  # PostgreSQL or MySQL
        with engine.connect() as conn:
            # Check the dialect to determine the database type
            db_type = engine.dialect.name

            if db_type == "postgresql":  # PostgreSQL
                result = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public';"))
            elif db_type == "mysql":  # MySQL
                result = conn.execute(text("SHOW TABLES"))
            else:
                raise ValueError(f"Unsupported database type: {db_type}")

            return [row[0] for row in result.fetchall()]

    else:
        raise ValueError(f"Unsupported engine type: {type(engine)}")

def set_index(conn, table_name):
    """
    Sets the index of a database table, this ensures efficient lookups.

    :param conn: connection to the database
    :param table_name: the name of the table you want to set the index for

    :raises ValueError: if the connection type is not supported
    """
    pms = get_primary_key(table_name)

    print(f"Setting index for table '{table_name}' and columns {pms}...")

    if isinstance(conn, sqlite3.Connection):  # SQLite
        cur = conn.cursor()
        for pm in pms:
            cur.execute(f"CREATE INDEX IF NOT EXISTS index_{pm} ON {table_name} ({pm});")

    elif isinstance(conn, Engine):  # PostgreSQL or MySQL
        db_type = conn.dialect.name  # Detect the database type
        with conn.connect() as engine:
            for pm in pms:
                if db_type == "postgresql":  # PostgreSQL
                    engine.execute(text(f"CREATE INDEX IF NOT EXISTS index_{pm} ON {table_name} ({pm});"))
                elif db_type == "mysql":  # MySQL
                    engine.execute(text(f"CREATE INDEX IF NOT EXISTS index_{pm} ON {table_name} ({pm});"))
                else:
                    raise ValueError(f"Unsupported database type for indexing: {db_type}")

    else:
        raise ValueError(f"Unsupported connection type: {type(conn)}")

def process_data_without_filter(conn):
    """
    Adds the reddit data to a database (sqlite or postgresql) without selecting specific lines, it just adds all the data.

    :param conn: connection to the database
    """
    if isinstance(conn, sqlite3.Connection):  # SQLite
        db_info_file = 'databases/db_info_sqlite_ALL.json'

    elif isinstance(conn, Engine):  # PostgreSQL or MySQL
        db_type = conn.dialect.name  # Detect database type

        if db_type == "postgresql":  # PostgreSQL
            db_info_file = 'databases/db_info_postgresql_ALL.json'

        elif db_type == "mysql":  # MySQL
            db_info_file = 'databases/db_info_mysql_ALL.json'

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

    else:
        raise ValueError(f"Only SQLite, PostgreSQL, and MySQL connections are supported, not {type(conn)}")

    print(f'Only adding new data. To rebuild existing tables, remove them from the {db_info_file} file')

    if not os.path.isfile(db_info_file):
        with open(db_info_file, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4)

    clean_json_duplicates(db_info_file)

    for table in get_tables(conn):
        delete_table = True
        with open(db_info_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for obj in data:
            if table in obj['success_tables']:
                delete_table = False
                break
        if delete_table:
            delete_table_db(table, conn)

    new_data_added = False
    table_columns = dict()

    # Preparing data
    for file in data_files:
        tables = data_files_tables[file]
        for table in tables:
            table_columns[table] = get_table_columns(sql_file_path='db_structure.sql', table_name=table)

    ignored_author_names = set()
    with open('ignored.txt', 'r', encoding='utf-8') as ignored:
        for ignored_name in ignored:
            ignored_author_names.add(ignored_name.strip().lower())

    # Add the data to the SQL database
    for file in data_files:
        tables = data_files_tables[file]

        tables_to_process = is_file_tables_added_db(file, tables, db_info_file)
        if tables_to_process:
            process_table(file, tables_to_process, conn, table_columns, ignored_author_names)
            add_file_table_db_info(file, tables_to_process, db_info_file)
            new_data_added = True
            for table in tables_to_process:
                set_index(conn, table)

    if not new_data_added:
        print('No new data was added. Removing duplicates...')
        time.sleep(0.1)  # Wait a bit so print does not interfere with the tqdm progress bar of removing duplicate
    remove_duplicates_db(conn, db_info_file)
