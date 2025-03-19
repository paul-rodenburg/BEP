import os
import re
import pickle
from general import extract_line
from tqdm import tqdm
from config import comments_file, posts_subset_base_name, comments_subset_file, LINES_SUBSET, dates_subsets, dates_to_original_file

# File paths
post_ids_path = f'cache/post_ids_{LINES_SUBSET}.pkl'


def check_existing_file() -> bool:
    """
    Check if comments subset file exists and prompt the user.

    :return: True if can continue
    """
    if os.path.isfile(comments_subset_file):
        user_continue = input(f'{comments_subset_file} already exists. Do you want to replace it? ([y]/n) ')
        if user_continue.lower() == 'n':
            return False
        else:
            os.remove(comments_subset_file)
            return True
    return True


def load_or_create_post_ids():
    """
    Load post IDs from cache or create the set from posts subset file.
    """
    os.makedirs('cache', exist_ok=True)
    id_pattern = re.compile(r'"id"\s*:\s*"([\w-]+)"')
    post_ids = set()

    if os.path.isfile(post_ids_path):
        with open(post_ids_path, 'rb') as f:
            post_ids = pickle.load(f)
    else:
        progress_bar_post_id = tqdm(desc=f'Making post id list', total=LINES_SUBSET)
        for i in range(len(dates_subsets)):
            date = dates_subsets[i]
            progress_bar_post_id.set_postfix_str(f'{date} ({i+1}/{len(dates_subsets)})')
            line_numbers = []

            # Collect all required line numbers
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
                            post_id = match.group(1)
                            post_ids.add(post_id)
                        target_index += 1  # Move to the next required line
                        progress_bar_post_id.update(1)

                    current_line_number += 1

        progress_bar_post_id.close()

        # Save post IDs to cache
        with open(post_ids_path, 'wb') as f:
            pickle.dump(post_ids, f)

    return post_ids


def find_parent_post(line):
    """
    Extract the parent post ID from a line in the comments file.

    :param line: The line from the comments file.

    :return: The parent post ID.
    """
    parent_id_pattern = re.compile(r'"parent_id"\s*:\s*"(\w+)"')
    match = parent_id_pattern.search(line)
    if match:
        parent_id = match.group(1)
        if '_' in parent_id:
            parent_id = parent_id.split('_')[1]
        return parent_id
    return None


def create_comment_subset(post_ids):
    """
    Create a subset of comments where the parent post is in the post IDs set.

    :param post_ids: The post IDs set.

    :return: The subset of comments where the parent post is in the post IDs set.
    """
    count_lines = 0
    count = 0
    with open(comments_file, 'r', encoding='utf-8') as f:
        progress_bar = tqdm(desc=f'Processing comments', total=LINES_SUBSET)

        for line in f:
            count += 1
            parent_id = find_parent_post(line)
            if parent_id and parent_id in post_ids:
                with open(comments_subset_file, 'a', encoding='utf-8') as f_out:
                    f_out.write(f"{count}\n")
                count_lines += 1
                progress_bar.update(1)

            if count_lines == LINES_SUBSET:
                break


def make_subset_comments():
    """
    Main function to run all steps for creating the comments subset.
    """
    # Subset files paths
    os.makedirs('data/subset', exist_ok=True)

    for date in dates_subsets:  # If post subsets don't exist, then exit
        if not os.path.isfile(f'{posts_subset_base_name}_{date}'):
            print(f'{posts_subset_base_name}_{date} does not exist. Make sure to create the post subsets first. Exiting...')
            return

    if check_existing_file():
        post_ids = load_or_create_post_ids()
        create_comment_subset(post_ids)
    else:
        print('Skipping creating comment subset...')


# Run the process
if __name__ == "__main__":
    # Update working directory
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)

    # Make folders
    os.makedirs('cache', exist_ok=True)
    os.makedirs('data/subset', exist_ok=True)

    make_subset_comments()
