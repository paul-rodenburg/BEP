import pandas as pd
import sqlite3
from config import posts_subset_base_name, LINES_SUBSET, comments_subset_file, rules_subset_file, wikis_subset_file, subreddits_subset_file, posts_2025_1_file, subset_files_tables, subset_to_original, dates_subsets, dates_to_original_file
from general import get_primary_key
from line_counts import get_line_count_file
from data_to_sql import clean_line
from tqdm import tqdm
import os
import time

def handle_author(line):
    return line

def process_cleaned_lines(cleaned_lines) -> pd.DataFrame:
    """"
    Converts list of cleaned lines into a DataFrame.
    :param cleaned_lines: list of cleaned lines

    :return DataFrame containing cleaned lines.
    """
    df_processed = pd.DataFrame(cleaned_lines)
    if len(df_processed) == 0 or df_processed is None:
        return pd.DataFrame()

    primary_key_column = get_primary_key(table)
    df_processed = df_processed.drop_duplicates(subset=primary_key_column)

    df_processed = df_processed.apply(lambda col: col.map(lambda x: str(x) if isinstance(x, list) else x))
    df_processed = df_processed.apply(lambda col: col.map(lambda x: str(x) if isinstance(x, dict) else x))

    return df_processed


def extract_lines(line_file, content_file, table) -> pd.DataFrame:
    """
    Extract the lines from the original data file using the lines in subset file efficiently.

    :param line_file: original lines file
    :param content_file: content file
    :param table: table name for the SQL database where data is going to be stored
    :return: DataFrame containing extracted lines.
    """
    lines_clean = []

    # Read and store line numbers in increasing order
    with open(line_file, 'r', encoding='utf-8') as fa:
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
                clean_lined = clean_line(line, table)
                lines_clean.append(clean_lined)
                target_index += 1  # Move to the next required line
                progress_bar.update(1)

            current_line_number += 1

    progress_bar.close()

    return process_cleaned_lines(lines_clean)


if __name__ == '__main__':
    db_file = 'data.db'
    if os.path.isfile(db_file):
        user_remove_db = input(f'{db_file} already exists. Do you want to remove it? [y/[n]]').strip().lower()
        if user_remove_db != 'y':
            exit(0)  # Dont go further because user doesnt want to replace the db file
        os.remove(db_file)
        print(f'Removed {db_file} file')
    conn = sqlite3.connect('data.db')

    subsets = [posts_subset_base_name, comments_subset_file, rules_subset_file, wikis_subset_file, subreddits_subset_file]

    for subset_line_file in subsets:
        if subset_line_file == posts_subset_base_name:
            files = [f'{posts_subset_base_name}_{date}' for date in dates_subsets]
            for file in files:
                for table in subset_files_tables[subset_line_file]:
                    if table in ['removed', 'banned']:
                        continue
                    date = file.split('_')[-1]
                    df = extract_lines(file, dates_to_original_file[date], table)
                    df.to_sql(table, conn, if_exists="append", index=False)
        else:
            original_file = subset_to_original[subset_line_file]

            for table in subset_files_tables[subset_line_file]:
                if table in ['removed', 'banned']:
                    continue
                df = extract_lines(subset_line_file, original_file, table)
                df.to_sql(table, conn, if_exists="append", index=False)