import orjson as json
from data_to_db.data_to_sql import write_json, load_json
from line_counts import get_line_count_file
from tqdm import tqdm
import os
import time
import gdown

def unnest_json(nested_json: dict) -> dict:
    """
    Flatten a nested JSON object, keeping only the final key names.
    If keys repeat at different levels, later ones will overwrite earlier ones.

    :param nested_json: The JSON object to flatten.
    :return: A flattened dictionary with only leaf keys.
    """
    flat_dict = {}

    def _unnest(obj):
        if isinstance(obj, dict):
            for key, value in obj.items():
                if isinstance(value, (dict, list)):
                    _unnest(value)
                else:
                    flat_dict[key] = value
        elif isinstance(obj, list):
            for item in obj:
                _unnest(item)

    _unnest(nested_json)
    return flat_dict


def find_max_char_lengths(ndjson_file, output_file, max_line_count, progress_bar):
    """
    Finds the maximum count of characters per attribute in a NDJSON file, writes this results to a json file.

    :param ndjson_file: the NDJSON file containing the data you want to count
    :param output_file: the JSON file where the results will be written to
    :param max_line_count: the maximum number of lines to process in the NDJSON file, set to None to process all lines
    :param progress_bar: a tqdm progress bar that displays the progress made in counting
    """
    max_lengths = {}
    count = 0
    current_data = {}
    if os.path.isfile(output_file):
        current_data = load_json(output_file)
    with open(ndjson_file, 'r', encoding='utf-8') as f:
        for line in f:
            progress_bar.update(1)
            line = line.strip()
            count += 1
            if not line:
                continue
            try:
                obj = json.loads(line)
                obj = unnest_json(obj)
                for key, value in obj.items():
                    value_str = str(value)
                    current_length = len(value_str)
                    if key not in max_lengths or current_length > max_lengths[key]:
                        max_lengths[key] = current_length
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line: {line}")
            if max_line_count is not None and count >= max_line_count:
                break

    # Write the results as JSON
    current_data[ndjson_file] = max_lengths

    # Add rule_id for subreddit_rules_2025-01
    if ndjson_file == 'data/subreddits/subreddit_rules_2025-01/subreddit_rules_2025-01':
        current_data['data/subreddits/subreddit_rules_2025-01/subreddit_rules_2025-01']['rule_id'] = 20
    write_json(current_data, output_file)


def generate_character_lengths(files_to_process_count: dict):
    """
    Counts the number of maximum characters per attribute per file.

    :param files_to_process: dict containing the files to process as key and as value the number of lines to process (can be less than the actual line count of this file)
    """
    # Setup progress bar and load the data file paths already processed
    all_lines = sum([v for v in files_to_process_count.values()])
    progress_bar = tqdm(total=all_lines, desc="Processing lines")
    current_files = set()
    if os.path.isfile('character_lengths.json'):
        current_json = load_json('character_lengths.json')
        current_files = set(current_json.keys())

    # Loop over files to process and find max characters per attribute
    count_files = 0
    for file, file_line_count in files_to_process_count.items():
        count_files += 1
        progress_bar.set_postfix_str(f"At file {count_files}/{len(files_to_process_count)}")
        if file in current_files:
            time.sleep(0.3)
            continue_user = input(f"\nFile {file} already exists. Do you want to process this file again? (y/n) ")
            if continue_user.lower().strip() == 'n':
                progress_bar.total -= file_line_count
                continue
        find_max_char_lengths(file, "character_lengths.json", max_line_count, progress_bar)


def download_character_lengths():
    """
    Download the character lengths file from Google Drive.
    """
    gdown.download('https://drive.google.com/file/d/1IjE--DCqy-zTdiAZiiTc2An1ksRIOlFk/view?usp=sharing', 'character_lengths.json', fuzzy=True)

if __name__ == '__main__':
    # Make a list containing the paths to the files needed to make the databases and get the max line count
    files = list(load_json('config.json')['data_files_tables'].keys())
    max_line_count = load_json('config.json')['maximum_rows_database']

    # Check if count file already exists and the data is about the same files that are needed to make the database
    if os.path.isfile('character_lengths.json'):
        files_processed = list(load_json('character_lengths.json').keys())
        if len(list(set(files) - set(files_processed))) == 0:
            print('All files are already processed!')
            print("To process files again, remove them from 'character_lengths.json'")
            exit(0)

    # Ask user if he/she wants to download (fastest option) or generate the count file
    download_or_generate = input("Do you want to download (d) or generate (g) the character lengths? (d/g) ")
    if download_or_generate.lower().strip() == 'd':
        # Because counting can take some time, downloading is faster
        download_character_lengths()

        # Check whether the files in character lengths are the same as the files needed to make the database.
        # If not, process the missing files
        with open('character_lengths.json', 'r', encoding='utf-8') as f:
            data = json.loads(f.read())
        if set(files) != set(data.keys()):
            # Get the list of missing files (file paths) and generate their count
            missing_files = list(set(files) - set(data.keys()))
            if len(missing_files) > 0:
                missing_files_count = {}
                for file in missing_files:
                    missing_files_count[file] = min(get_line_count_file(file), max_line_count)
                generate_character_lengths(missing_files_count)
    else:
        # Prepare a dict with keys the file paths and as value the line count to process
        # (can be less than actual line count if max_line_count is less) of the files to process
        files_count = {}
        for file in files:
            files_count[file] = min(get_line_count_file(file), max_line_count)
        generate_character_lengths(files_count)
