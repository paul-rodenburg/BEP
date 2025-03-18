import sys
import pandas as pd
import sqlite3
import ujson as json
import re
from config import posts_subset_file, subset_files_tables, comments_subset_file, rules_subset_file, wikis_subset_file, subreddits_subset_file, LINES_SUBSET
from tqdm import tqdm
from general import get_primary_key
from line_counts import get_line_count_file
from datetime import datetime

subset_folder = 'data/subset'

def get_ignore_usernames() -> set:
    """
    Gets a set containing usernames to be ignored (bots, moderators, deleted users) from ignored.txt

    :return: set of usernames to be ignored
    """
    ignore_names = set()
    with open('ignored.txt', 'r') as f:
        for line in f:
            ignore_names.add(line.strip())
    return ignore_names


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

clean_errors = 0

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

    cleaned_line = {key: line[key] for key in items_to_keep if key in line}

    return cleaned_line

def table_exists(conn, table_name):
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    return conn.execute(query, (table_name,)).fetchone() is not None

if __name__ == '__main__':
    cleaned_lines = []
    totals = {posts_subset_file: LINES_SUBSET,
              comments_subset_file: LINES_SUBSET,
              rules_subset_file: get_line_count_file(rules_subset_file),
              wikis_subset_file: get_line_count_file(wikis_subset_file),
              subreddits_subset_file: get_line_count_file(subreddits_subset_file)}

    conn = sqlite3.connect('data.db')
    ignore_names = get_ignore_usernames()

    for subset_file, tables in subset_files_tables.items():
        print(f'\nCleaning {subset_file.split("/")[-1]}')
        if len(tables) == 0:
            continue
        for table in tables:
            authors = set()
            clean_errors = 0

            # Skip table if it already exists
            # if table_exists(conn, table) and table not in ['removed', 'post']:
            if table_exists(conn, table):
                print(f"Skipping {table}, already exists.")
                continue

            with open(subset_file, 'r', encoding='utf-8') as f:
                for line in tqdm(f, desc=f'Creating {table}', total=totals[subset_file]):
                    if table == 'author':
                        author_fullname_pattern = re.compile(r'"author_fullname"\s*:\s*"([^"]+)"')
                        author_username_pattern = re.compile(r'"author"\s*:\s*"([^"]+)"')
                        try:
                            author_fullname = author_fullname_pattern.search(line).group(1) # ID of author (username != ID)
                            author_username = author_username_pattern.search(line).group(1) # Username of author (username != ID)
                            if author_fullname in authors or author_username in ignore_names: # Don't add already added authors and ignored authors
                                continue
                            authors.add(author_fullname)
                        except:  # author_fullname could not be found, author is likely to be deleted so don't add to database
                            continue
                    cleaned_line = clean_line(line, table)
                    cleaned_lines.append(cleaned_line)

            cleaned_lines = [i for i in cleaned_lines if i is not None] # Removing Nones (this lead to an error)
            print(f'{clean_errors:,} errors found in table {table}')
            df = pd.DataFrame(cleaned_lines)
            df = df.apply(lambda col: col.map(lambda x: str(x) if isinstance(x, list) else x))
            df = df.apply(lambda col: col.map(lambda x: str(x) if isinstance(x, dict) else x))

            # Remove duplicates
            primary_key_column = get_primary_key(table)
            try:
                df = df.drop_duplicates(subset=primary_key_column)
            except:
                print(cleaned_lines[:20])
                print(primary_key_column)
                print(df[df['author'] != '[deleted]'])
                sys.exit(1)

            if len(df) == 0:  # Don't make a table if there is no data
                print(f'TABLE {table} IS EMPTY!')
                continue
            df.to_sql(table, conn, if_exists="replace", index=False)

            # Cleanup
            df = pd.DataFrame()
            cleaned_lines = []
