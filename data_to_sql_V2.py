import os
import pandas as pd
import sqlite3
from config import posts_subset_file, LINES_SUBSET, comments_subset_file, rules_subset_file, wikis_subset_file, subreddits_subset_file, posts_2025_file, subset_files_tables, subset_to_original
from general import get_line_count_file, get_primary_key
from data_to_sql import clean_line
from tqdm import tqdm

def handle_author(line):
    return line

def process_cleaned_lines(cleaned_lines) -> pd.DataFrame:
    """"
    Converts list of cleaned lines into a DataFrame.
    :param cleaned_lines: list of cleaned lines

    :return DataFrame containing cleaned lines.
    """
    df_processed = pd.DataFrame(cleaned_lines)
    primary_key_column = get_primary_key(table)
    df_processed = df_processed.drop_duplicates(subset=primary_key_column)

    df_processed = df_processed.apply(lambda col: col.map(lambda x: str(x) if isinstance(x, list) else x))
    df_processed = df_processed.apply(lambda col: col.map(lambda x: str(x) if isinstance(x, dict) else x))

    return df_processed


def extract_lines(line_file, content_file, table) -> pd.DataFrame:
    """"
    Extract the lines from the original data file using the lines in subset file.

    :param line_file: original lines file
    :param content_file: content file
    :param table: table name for the SQL database where data is going to be stored
    :param nr_lines: number of lines to extract (only used for tqdm progress bar)
    :return DataFrame containing lines.
    """
    lines_clean = []
    with open(line_file, 'r', encoding='utf-8') as fa, open(content_file, 'r', encoding='utf-8') as fb:
        line_numbers = (int(line.strip()) for line in fa)
        current_line_number = next(line_numbers, None)  # Get first line number

        fb_lines = fb.readlines()
        total_lines_fb = len(fb_lines)

        for i, line in tqdm(enumerate(fb_lines, start=1), total=total_lines_fb, desc=f"Processing {table}"):
            while current_line_number is not None and i == current_line_number:
                clean_lined = clean_line(line, table)
                lines_clean.append(clean_lined)
                current_line_number = next(line_numbers, None)  # Get next line number

                if current_line_number is None:
                    df_clean = process_cleaned_lines(lines_clean)
                    return df_clean

    df_clean = process_cleaned_lines(lines_clean)
    return df_clean

if __name__ == '__main__':
    conn = sqlite3.connect('data.db')

    totals = {posts_subset_file: LINES_SUBSET,
              comments_subset_file: LINES_SUBSET,
              rules_subset_file: get_line_count_file(rules_subset_file),
              wikis_subset_file: get_line_count_file(wikis_subset_file),
              subreddits_subset_file: get_line_count_file(subreddits_subset_file)}

    for subset_line_file, total_lines in totals.items():
        original_file = subset_to_original[subset_line_file]

        for table in subset_files_tables[subset_line_file]:
            df = extract_lines(subset_line_file, original_file, table)
            df.to_sql(table, conn, if_exists="replace", index=False)