from datetime import datetime
import pandas as pd
import json
import os
import time
import math
from itertools import chain
from pandas.core.internals.construction import dataclasses_to_dicts
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

def get_table_columns(sql_file_path, table_name):
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
            dt = datetime.fromisoformat(line['revision_date'].replace("Z", "+00:00"))
            epoch_time = int(dt.timestamp())
            line['revision_date'] = epoch_time
            pattern = r"/r/([^/]+)"
            subreddit_match = re.search(pattern, text)
            if not subreddit_match:  # If the subreddit could not be found then return None since this data will be useless
                continue
            line['subreddit'] = subreddit_match.group(1)

        if table == 'post':  # To not get any postgreSQL errors
            if int(line['edited']) == 0:
                line['edited'] = False
            else:
                line['edited'] = True

        if table == 'author':
            if line['author'] in ignored_author_names:
                continue

        cleaned_line = {key: line[key] for key in items_to_keep if key in line}
        cleaned_data[table] = cleaned_line

    return cleaned_data

def process_cleaned_lines(cleaned_lines_dct) -> dict[str, pd.DataFrame]:

    for table_name, data in cleaned_lines_dct.items():
        if data is None:
            continue

        cleaned_lines_dct[table_name] = pd.DataFrame(data)
        if len(cleaned_lines_dct[table_name]) == 0:
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

    return cleaned_lines_dct


sql_count = 0

def process_table(data_file, tables, conn, table_columns, ignored_author_names, chunk_size=10_000):
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
    global progress_bar
    lines_clean = {}
    for table_name in tables:
        lines_clean[table_name] = []


    total_lines = get_line_count_file(data_file)
    progress_bar = tqdm(total=total_lines, desc=f"Processing {len(tables)} tables: {tables} (from {data_file.split('/')[-1]})")

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
    global sql_count, progress_bar
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=5000)
    sql_count += 1
    progress_bar.set_postfix_str(f'[{sql_count}/{math.ceil(progress_bar.total / chunk_size * len_tables):,} SQL writes]')


def is_file_tables_added_db(data_file, table, db_info_file):
    if os.path.isfile(db_info_file):
        with open(db_info_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for obj in data:
            if obj['file'] == data_file:
                if set(table) == set(obj['success_tables']):
                    return True
    return False


def add_file_table_db_info(subset_file, tables, db_info_file):
    if not os.path.isfile(db_info_file):
        with open(db_info_file, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4)

    with open(db_info_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    file_entry = next((obj for obj in data if obj['file'] == subset_file), None)

    if file_entry:
        if tables not in file_entry['success_tables']:
            file_entry['success_tables'].append(tables)
            file_entry['success_tables'] = list(chain.from_iterable(file_entry['success_tables']))
            file_entry['success_tables'] = list(set(file_entry['success_tables']))
    else:
        data.append({'file': subset_file, 'success_tables': [tables]})

    with open(db_info_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


def remove_duplicates_db(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table in tqdm(tables, desc='Removing duplicates from database'):
        table_name = table[0]
        if table_name in ['post', 'comment']:
            continue
        q = f'SELECT * FROM {table_name}'
        df_clean = pd.read_sql_query(q, conn)
        try:
            df_clean = df_clean.drop_duplicates(subset=get_primary_key(table_name))
        except:
            print(f'Error with table {table_name} and pm: {get_primary_key(table_name)}')
            exit(1)
        df_clean.to_sql(table_name, conn, if_exists='replace', index=False)
        df_clean = pd.DataFrame()


def delete_table_db(table_name, engine):
    print(f'Deleting table {table_name}...')
    if isinstance(engine, sqlite3.Connection):
        engine.execute(f"DROP TABLE IF EXISTS {table_name}")
        print(f'Deleted table {table_name} in SQLITE database')
    elif isinstance(engine, Engine):
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
        print(f'Deleted table {table_name} in PostgreSQL database')
    else:
        raise ValueError(f'{type(engine)} is not supported')


def get_tables(engine):
    # Establish a connection from the SQLAlchemy engine
    if isinstance(engine, sqlite3.Connection):
        result = engine.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = result.fetchall()
        return [table[0] for table in tables]
    elif isinstance(engine, Engine):
        with engine.connect() as conn:
            result = conn.execute(text("SELECT tablename FROM pg_tables WHERE schemaname = 'public';"))
            return [row[0] for row in result.fetchall()]
    else:
        raise ValueError(f'{type(engine)} is not supported')


def process_data_without_filter(conn):
    if isinstance(conn, sqlite3.Connection):  # SQLite connection
        db_info_file = 'databases/db_info_sqlite_ALL.json'
    elif isinstance(conn, Engine):  # PostgreSQL connection
        db_info_file = 'databases/db_info_post_ALL.json'
    else:
        raise ValueError(f'Only SQLite and Postgres connections are supported, not {type(conn)}')

    print(f'Only adding new data. To rebuild existing tables, remove them from the {db_info_file} file')

    if not os.path.isfile(db_info_file):
        with open(db_info_file, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4)
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
            ignored_author_names.add(ignored_name.strip())

    # Add the data to the SQL database
    for file in data_files:
        tables = data_files_tables[file]
        if is_file_tables_added_db(file, tables, db_info_file):
            continue
        process_table(file, tables, conn, table_columns, ignored_author_names)
        add_file_table_db_info(file, tables, db_info_file)
        new_data_added = True


    if not new_data_added:
        print('No new data was added. Removing duplicates...')
        time.sleep(0.1)  # Wait a bit so print does not interfere with the tqdm progress bar of removing duplicates
    remove_duplicates_db(conn)
