import os
import ujson as json
from tqdm import tqdm
from config import *
from general import get_line_count_file

def check_existing_file() -> list:
    """
    Check if posts subset file exists and prompt the user.

    :return: a list of dates to skip
    """
    subset_files = [f'{posts_subset_file}_{date}' for date in dates_subsets]

    # Check if any file exists
    existing_files = [file for file in subset_files if os.path.isfile(file)]
    skip_dates = []

    for existing_file in existing_files:
        date_file = existing_file.split('_')[-1]
        user_continue = input(f"The post file for date {date_file}. Do you want to replace them? ([y]/n) ").strip().lower()
        if user_continue == 'n':
            continue
        skip_dates.append(date_file)

    return skip_dates


def find_non_empty_nested_keys(obj, parent_key=""):
    """Recursively find non-empty nested JSON objects (length > 0), excluding items in list excluding."""
    nested_keys = []

    excluding = ['_meta', 'author_flair_richtext', 'link_flair_richtext', 'preview', 'crosspost_parent_list']

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key in excluding:  # Exclude keys in the excluding list
                continue
            if isinstance(value, dict) and len(value) > 0:  # Non-empty dictionary
                nested_keys.append(parent_key + key if parent_key else key)
                nested_keys.extend(find_non_empty_nested_keys(value, parent_key + key + "."))  # Recurse
            elif isinstance(value, list):  # List containing non-empty dicts
                for i, item in enumerate(value):
                    if isinstance(item, dict) and len(item) > 0:
                        nested_keys.append(parent_key + key if parent_key else key)
                        nested_keys.extend(find_non_empty_nested_keys(item, parent_key + key + f"[{i}]."))  # Recurse

    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            if isinstance(item, dict) and len(item) > 0:
                nested_keys.extend(find_non_empty_nested_keys(item, parent_key + f"[{i}]."))  # Recurse

    return nested_keys

def init_count_lines(skip_dates) -> int:
    if len(skip_dates) == 0:
        return 0
    files_to_count = [f'{posts_subset_file}_{i}' for i in dates_to_subset[skip_dates]]
    count_lines = 0
    for file in files_to_count:
        count_lines += get_line_count_file(file)
    return count_lines


def create_post_subset_file(skip_dates):
    count_lines = init_count_lines(skip_dates)
    print(f'count_lines init to {count_lines:,}')
    progress_bar = tqdm(desc='Processing posts', total=LINES_SUBSET)

    def process_file(file_path, date):
        """Helper function to process a file and extract lines."""
        nonlocal count_lines
        count = 0
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                count += 1
                line_json = json.loads(line)  # Parse JSON
                nested_keys = find_non_empty_nested_keys(line_json)  # Find non-empty nested JSON

                if nested_keys:  # Only process if nested objects exist
                    count_lines += 1
                    with open(f'{posts_subset_file}_{date}', 'a', encoding='utf-8') as f_out:
                        f_out.write(f"{count}\n")

                    progress_bar.update(1)

                if count_lines == LINES_SUBSET:
                    return  # Stop processing if limit is reached

    dates_subsets_processed = [i for i in dates_subsets if i not in skip_dates]

    for i in range(len(dates_subsets_processed)):
        subset_file = dates_to_subset[dates_subsets_processed[i]]
        process_file(subset_file, dates_subsets_processed[i])
        if count_lines < LINES_SUBSET and i != len(dates_subsets_processed) - 1:
            progress_bar.set_postfix_str(f'{dates_subsets_processed[i]} had not enough posts. Continuing with {dates_subsets_processed[i+1]}')
        else:
            print(f'{dates_subsets_processed[i]} had not enough posts (wanted: {LINES_SUBSET:,}; got: {count_lines:,}) ')
            break

    progress_bar.close()




def make_post_subset():
    # Subset files paths
    os.makedirs('data/subset', exist_ok=True)

    skip_dates = check_existing_file()
    create_post_subset_file(skip_dates)


if __name__ == '__main__':
    # Update working directory
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)

    # Make folders
    os.makedirs('cache', exist_ok=True)
    os.makedirs('data/subset', exist_ok=True)

    make_post_subset()