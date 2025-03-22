import pandas as pd
import sqlite3
from config import posts_subset_base_name, LINES_SUBSET, comments_subset_file, rules_subset_file, wikis_subset_file, subreddits_subset_file, posts_2025_1_file, subset_files_tables, subset_to_original, dates_subsets, dates_to_original_file
from general import get_primary_key
from line_counts import get_line_count_file
from data_to_sql import clean_line
from tqdm import tqdm
import os
import time
import json
import math

progress_bar = None

def handle_author(line):
    return line

def process_cleaned_lines(cleaned_lines, table) -> pd.DataFrame:
    """"
    Converts list of cleaned lines into a DataFrame.
    :param cleaned_lines: list of cleaned lines
    :param table: table name

    :return DataFrame containing cleaned lines.
    """
    df_processed = pd.DataFrame(cleaned_lines)
    if len(df_processed) == 0 or df_processed is None:
        return pd.DataFrame()

    primary_key_column = get_primary_key(table)
    df_processed = df_processed.drop_duplicates(subset=primary_key_column)

    df_processed = df_processed.map(lambda x: str(x) if isinstance(x, (list, dict)) else x)

    return df_processed

def process_table(subset_file, original_file, table, conn, chunk_size=10_000, wiki=False):
    for df_chunk in extract_lines(subset_file, original_file, table, wiki, chunk_size):
        if not df_chunk.empty:
            write_to_db(df_chunk, table, conn, chunk_size=chunk_size)


def extract_lines(line_file, content_file, table, wiki=False, chunk_size=10_000) -> pd.DataFrame:
    global progress_bar
    """
    Extract the lines from the original data file using the lines in subset file efficiently.

    :param line_file: original lines file
    :param content_file: content file
    :param table: table name for the SQL database where data is going to be stored
    :param wiki: if set to True then subreddit name is also in lien file (seperated from lines by a -)
    :param chunk_size: number of lines to read at a time
    :return: DataFrame containing extracted lines.
    """
    lines_clean = []

    # Read and store line numbers in increasing order
    with open(line_file, 'r', encoding='utf-8') as fa:
        if wiki:
            line_numbers = []
            subreddits_wiki = []
            for line in fa:
                line_numbers.append(int(line.strip().split(' - ')[0]))
                subreddits_wiki.append(str(line.strip().split(' - ')[-1]))
            if len(subreddits_wiki) != len(subreddits_wiki):  # Abort
                print('ERROR! Len subreddits_wiki does not match subreddits_wiki.')
                return pd.DataFrame()
        else:
            line_numbers = [int(line.strip()) for line in fa if line.strip().isdigit()]

    total_lines = get_line_count_file(line_file)
    time.sleep(0.2)  # Wait a bit so print of line count doesnt interfere with tqdm progress bar
    progress_bar = tqdm(total=total_lines, desc=f"Processing {table}")

    with open(content_file, 'r', encoding='utf-8') as fb:
        current_line_number = 0
        target_index = 0  # Index for tracking required line numbers

        for line in fb:
            if target_index >= total_lines:
                break  # Stop when all required lines are processed

            if current_line_number == line_numbers[target_index]:
                cleaned_line = clean_line(line, table)
                if table == 'wiki':
                    cleaned_line['subreddit'] = subreddits_wiki[target_index]
                lines_clean.append(cleaned_line)
                target_index += 1  # Move to the next required line
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
    progress_bar.set_postfix_str(f'[{sql_count}/{math.ceil(progress_bar.total/chunk_size)} SQL writes]')


def is_file_table_added_db(subset_file, table, db_info_file='db_info.json'):
    if os.path.isfile(subset_file):
        with open(db_info_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for obj in data:
            if obj['file'] == subset_file:
                if table in obj['success_tables']:
                    return True
    return False

def add_file_table_db_info(subset_file, table, db_info_file='db_info.json'):
    # Ensure the JSON file exists
    if not os.path.isfile(db_info_file):
        with open(db_info_file, 'w', encoding='utf-8') as f:
            json.dump([], f, indent=4)

    # Load the existing JSON data
    with open(db_info_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Check if the file entry already exists
    file_entry = next((obj for obj in data if obj['file'] == subset_file), None)

    if file_entry:
        # If the table is not already in success_tables, add it
        if table not in file_entry['success_tables']:
            file_entry['success_tables'].append(table)
            file_entry['success_tables'] = list(set(file_entry['success_tables']))  # Make sure there are no duplicates
    else:
        # If file doesn't exist, create a new entry
        data.append({'file': subset_file, 'success_tables': [table]})

    # Write back the updated data
    with open(db_info_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


def remove_duplicates_db(conn):
    cursor = conn.cursor()

    # Get a list of all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    # Loop over each table
    for table in tqdm(tables, desc='Removing duplicates from database'):
        table_name = table[0]
        if table_name in ['post']:
            continue  # There will be no duplicates in the posts and comments
        q = f'SELECT * FROM {table_name}'
        df_clean = pd.read_sql_query(q, conn)
        df_clean = df_clean.drop_duplicates(subset=get_primary_key(table_name))
        df_clean.to_sql(table_name, conn, if_exists='replace', index=False)
        df_clean = pd.DataFrame()  # Clean memory

def delete_table_db(table_name, conn):
    print(f'Deleting table {table_name}...')
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")


if __name__ == '__main__':
    db_info_file = 'db_info.json'
    if os.path.isfile(db_info_file):
        with open(db_info_file, 'r', encoding='utf-8') as fb:
            db_info = json.load(fb)
    else:
        db_info = []
    db_file = 'data.db'
    if os.path.isfile(db_file):
        user_action = input(f'{db_file} already exists. Choose an action: replace [r] add new data only [a]').strip().lower()
        if user_action == 'a':
            pass
        elif user_action == 'r':
            os.remove(db_file)
            os.remove(db_info_file)
            print(f'Removed {db_file} file')
        else:
            print(f'{user_action} not recognised. Exiting...')
            exit(0)
    conn = sqlite3.connect('data.db')

    subsets = [posts_subset_base_name, comments_subset_file, rules_subset_file, wikis_subset_file, subreddits_subset_file]

    new_data_added = False
    for subset_line_file in subsets:
        if subset_line_file == posts_subset_base_name:
            files = [f'{posts_subset_base_name}_{date}' for date in dates_subsets]
            for file in files:
                for table in subset_files_tables[subset_line_file]:
                    if table in ['removed', 'banned']:
                        continue
                    if is_file_table_added_db(file, table):
                        continue
                    date = file.split('_')[-1]
                    process_table(file, dates_to_original_file[date], table, conn)
                    add_file_table_db_info(file, table)
                    new_data_added = True
        else:
            original_file = subset_to_original[subset_line_file]

            for table in subset_files_tables[subset_line_file]:
                if table in ['removed', 'banned']:
                    continue
                if is_file_table_added_db(subset_line_file, table):
                    continue
                if 'wiki' in table:
                    process_table(subset_line_file, original_file, table, conn, wiki=True)
                else:
                    process_table(subset_line_file, original_file, table, conn)
                add_file_table_db_info(subset_line_file, table)
                new_data_added = True

    if not new_data_added:
        print('No new data was added. Removing duplicates...')
        time.sleep(0.2)  # Wait a bit so print does not interfere with the tqdm progress bar of removing duplicates
    remove_duplicates_db(conn)
