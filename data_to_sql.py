from datetime import datetime

import pandas as pd
import json
import os
import time
import math

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy import text
from tqdm import tqdm
from general import get_primary_key
from line_counts import get_line_count_file
from config import posts_subset_base_name, LINES_SUBSET, comments_subset_file, rules_subset_file, wikis_subset_file, \
    subreddits_subset_file, posts_2025_1_file, subset_files_tables, subset_to_original, dates_subsets, \
    dates_to_original_file
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

def clean_line(line, table_name, sql_file="db_structure.sql"):
    global clean_errors

    try:
        line = json.loads(line)
    except:
        clean_errors += 1
        return None
    # Change this to your actual .sql file path
    items_to_keep = get_table_columns(sql_file, table_name)
    if table_name in 'banned':
        if line['banned_at_utc'] is None and line['banned_by'] is None:
            return None
    if table_name == 'removed':
        if line['removal_reason'] is None and line['removed_by'] is None:
            return None
    if table_name == 'wiki':
        dt = datetime.fromisoformat(line['revision_date'].replace("Z", "+00:00"))
        epoch_time = int(dt.timestamp())
        line['revision_date'] = epoch_time
    if table_name == 'post':
        if int(line['edited']) == 0:
            line['edited'] = False
        else:
            line['edited'] = True

    cleaned_line = {key: line[key] for key in items_to_keep if key in line}

    return cleaned_line

def process_cleaned_lines(cleaned_lines, table) -> pd.DataFrame:
    df_processed = pd.DataFrame(cleaned_lines)
    if len(df_processed) == 0 or df_processed is None:
        return pd.DataFrame()

    primary_key_column = get_primary_key(table)
    columns = df_processed.columns.tolist()
    for pm in primary_key_column:
        if pm not in columns:
            return pd.DataFrame()
    df_processed = df_processed.drop_duplicates(subset=primary_key_column)
    df_processed = df_processed.map(lambda x: str(x) if isinstance(x, (list, dict)) else x)

    return df_processed


def process_table(subset_file, original_file, table, conn, chunk_size=10_000, wiki=False):
    global sql_count
    added_count = 0
    for df_chunk in extract_lines(subset_file, original_file, table, wiki, chunk_size):
        if not df_chunk.empty:
            write_to_db(df_chunk, table, conn, chunk_size=chunk_size)
            added_count += 1
    sql_count = 0  # Reset count for the progress bar
    if added_count == 0:
        print(f'Error! All chunks of {table} were empty')


def extract_lines(line_file, content_file, table, wiki=False, chunk_size=10_000) -> pd.DataFrame:
    global progress_bar
    lines_clean = []

    with open(line_file, 'r', encoding='utf-8') as fa:
        if wiki:
            line_numbers = []
            subreddits_wiki = []
            for line in fa:
                line_numbers.append(int(line.strip().split(' - ')[0]))
                subreddits_wiki.append(str(line.strip().split(' - ')[-1]))
            if len(subreddits_wiki) != len(subreddits_wiki):
                print('ERROR! Len subreddits_wiki does not match subreddits_wiki.')
                return pd.DataFrame()
        else:
            line_numbers = [int(line.strip()) for line in fa if line.strip().isdigit()]

    total_lines = get_line_count_file(line_file)
    time.sleep(0.2)
    progress_bar = tqdm(total=total_lines, desc=f"Processing {table}")

    ignored_author_names = set()
    with open('ignored.txt', 'r', encoding='utf-8') as ignored:
        for ignored_name in ignored:
            ignored_author_names.add(ignored_name.strip())

    with open(content_file, 'r', encoding='utf-8') as fb:
        current_line_number = 0
        target_index = 0

        for line in fb:
            if target_index >= total_lines:
                break

            if current_line_number == line_numbers[target_index]:
                cleaned_line = clean_line(line, table)
                if table == 'wiki':
                    cleaned_line['subreddit'] = subreddits_wiki[target_index]
                if table == 'author':
                    if cleaned_line['author'] in ignored_author_names:
                        continue
                lines_clean.append(cleaned_line)
                target_index += 1
                progress_bar.update(1)

                if len(lines_clean) >= chunk_size:
                    yield process_cleaned_lines(lines_clean, table)
                    lines_clean = []
            current_line_number += 1

    progress_bar.close()
    if lines_clean:
        yield process_cleaned_lines(lines_clean, table)


sql_count = 0


def write_to_db(df, table, conn, chunk_size=10_000):
    global sql_count, progress_bar
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=5000)
    sql_count += 1
    progress_bar.set_postfix_str(f'[{sql_count}/{math.ceil(progress_bar.total / chunk_size)} SQL writes]')


def is_file_table_added_db(subset_file, table, db_info_file):
    if os.path.isfile(db_info_file):
        with open(db_info_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for obj in data:
            if obj['file'] == subset_file:
                if table in obj['success_tables']:
                    return True
    return False


def add_file_table_db_info(subset_file, table, db_info_file):
    if not os.path.isfile(db_info_file):
        with open(db_info_file, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4)

    with open(db_info_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    file_entry = next((obj for obj in data if obj['file'] == subset_file), None)

    if file_entry:
        if table not in file_entry['success_tables']:
            file_entry['success_tables'].append(table)
            file_entry['success_tables'] = list(set(file_entry['success_tables']))
    else:
        data.append({'file': subset_file, 'success_tables': [table]})

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


def process_data(conn):
    if isinstance(conn, sqlite3.Connection):  # SQLite connection
        db_info_file = 'databases/db_info_sqlite.json'
    elif isinstance(conn, Engine):  # PostgreSQL connection
        db_info_file = 'databases/db_info_post.json'
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

    subsets = [posts_subset_base_name, comments_subset_file, rules_subset_file, wikis_subset_file,
               subreddits_subset_file]
    new_data_added = False

    for subset_line_file in subsets:
        if subset_line_file == posts_subset_base_name:
            files = [f'{posts_subset_base_name}_{date}' for date in dates_subsets]
            for file in files:
                for table in subset_files_tables[subset_line_file]:
                    if table in ['removed', 'banned']:
                        continue
                    if is_file_table_added_db(file, table, db_info_file):
                        continue
                    date = file.split('_')[-1]
                    process_table(file, dates_to_original_file[date], table, conn)
                    add_file_table_db_info(file, table, db_info_file)
                    new_data_added = True
        else:
            original_file = subset_to_original[subset_line_file]

            for table in subset_files_tables[subset_line_file]:
                if table in ['removed', 'banned']:
                    continue
                if is_file_table_added_db(subset_line_file, table, db_info_file):
                    continue
                if 'wiki' in table:
                    process_table(subset_line_file, original_file, table, conn, wiki=True)
                else:
                    process_table(subset_line_file, original_file, table, conn)
                add_file_table_db_info(subset_line_file, table, db_info_file)
                new_data_added = True

    if not new_data_added:
        print('No new data was added. Removing duplicates...')
        time.sleep(0.2)  # Wait a bit so print does not interfere with the tqdm progress bar of removing duplicates
    remove_duplicates_db(conn)
