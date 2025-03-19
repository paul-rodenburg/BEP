import os
import re
import pickle
import ujson as json
from tqdm import tqdm
from line_counts import get_line_count_file
from config import (
    posts_subset_base_name, LINES_SUBSET,
    subreddit_rules_file, rules_subset_file, subreddit_wiki_file,
    subreddits_file, wikis_subset_file, subreddits_subset_file, dates_subsets, dates_to_original_file
)
import time


def check_existing_files(list_subreddits):
    """Check if subreddit subset files exist and prompt the user for action."""
    skip_list = []
    os.makedirs('cache', exist_ok=True)
    for k, v in list_subreddits.items():
        file_path = v['file']
        if os.path.isfile(file_path):
            user_continue = input(
                f'The file {file_path.split("/")[-1]} already exists. Choose an option: skip [s] replace [r]: ')
            if user_continue.lower() == 's':
                skip_list.append(k)
            elif user_continue.lower() == 'r':
                os.remove(file_path)
            else:
                print(f'"{user_continue}" is not a valid option. Skipping...')
                skip_list.append(k)
    return skip_list


def load_or_create_subreddit_cache(cache_file):
    """Load subreddit names from cache or extract them from the posts subset file efficiently."""
    id_pattern = re.compile(r'"subreddit_name_prefixed"\s*:\s*"r/([^"]+)"')

    if os.path.isfile(cache_file):
        with open(cache_file, 'rb') as f:
            return pickle.load(f)

    subreddits = set()
    line_numbers = []
    progress_bar = tqdm(desc=f'Making subreddit set', total=LINES_SUBSET)

    for i in range(len(dates_subsets)):
        date = dates_subsets[i]
        progress_bar.set_postfix_str(f'{date} ({i+1}/{len(dates_subsets)})')
        with open(f'{posts_subset_base_name}_{date}', 'r', encoding='utf-8') as f:
            line_numbers.extend(int(line.strip()) for line in f if line.strip().isdigit())

        # Read the content file once
        with open(dates_to_original_file[date], 'r', encoding='utf-8') as content_file:
            current_line_number = 0
            target_index = 0  # Index for tracking required line numbers

            for content in content_file:
                if target_index >= len(line_numbers):  # Stop when all required lines are processed
                    break

                if current_line_number == line_numbers[target_index]:
                    match = id_pattern.search(content)
                    if match:
                        subreddit = match.group(1)  # Extract subreddit name
                        subreddits.add(subreddit)
                    target_index += 1  # Move to the next required line
                    progress_bar.update(1)

                current_line_number += 1

    progress_bar.close()

    # Save subreddits to cache
    with open(cache_file, 'wb') as f:
        pickle.dump(subreddits, f)

    return subreddits


def create_subreddit_subsets(list_subreddits, subreddits, skip_list):
    """Create subreddit subsets"""
    for k, v in list_subreddits.items():
        if k in skip_list:
            continue  # Skip processing if user chose to skip

        NUMBER_LINES_RULES = get_line_count_file(k)
        time.sleep(0.2)  # Wait a bit so prints dont interfere with tqdm progress bar
        subreddit_pattern = re.compile(v['regex'])
        count_line = 0
        with open(k, 'r', encoding='utf-8') as f:
            with open(v['file'], 'a', encoding='utf-8') as f_out:
                for line in tqdm(f, desc=f'Making {k.split("/")[-1]} subset', total=NUMBER_LINES_RULES):
                    count_line += 1
                    if k == subreddit_wiki_file:
                        data = json.loads(line)
                        match = re.search(r"(?<=/r/)[^/]+", data['path'])
                        if match:
                            subreddit = match.group()
                            if subreddit in subreddits:
                                f_out.write(f"{count_line} - {subreddit}\n")
                    else:
                        match = subreddit_pattern.search(line)
                        if match:
                            subreddit = match.group(1)[2:]  # Remove r/ prefix
                            if subreddit in subreddits:
                                f_out.write(f"{count_line}\n")


def make_subset_subreddits():
    """Main function to create subreddit subsets."""
    list_subreddits = {
        subreddit_rules_file: {'file': rules_subset_file, 'regex': r'"subreddit"\s*:\s*"([^"]+)"'},
        subreddit_wiki_file: {'file': wikis_subset_file, 'regex': r'"subreddit"\s*:\s*"([^"]+)"'},
        subreddits_file: {'file': subreddits_subset_file, 'regex': r'"display_name_prefixed"\s*:\s*"([^"]+)"'}
    }

    for date in dates_subsets:  # If post subsets don't exist, then exit
        if not os.path.isfile(f'{posts_subset_base_name}_{date}'):
            print(f'{posts_subset_base_name}_{date} does not exist. Make sure to create the post subsets first. Exiting...')
            return

    cache_file = f'cache/subreddit_names_{LINES_SUBSET}.pkl'
    skip_list = check_existing_files(list_subreddits)

    if len(skip_list) == len(list_subreddits):
        print('You chose to skip all. Exiting...')
        return

    subreddits = load_or_create_subreddit_cache(cache_file)
    create_subreddit_subsets(list_subreddits, subreddits, skip_list)


# Run the process
if __name__ == "__main__":
    # Update working directory
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)

    # Make folders
    os.makedirs('cache', exist_ok=True)
    os.makedirs('data/subset', exist_ok=True)

    make_subset_subreddits()