import os
import sys
import re
import pickle
from tqdm import tqdm
from config import comments_file, posts_subset_file, comments_subset_file, LINES_SUBSET

# File paths
post_ids_path = f'cache/post_ids_{LINES_SUBSET}.pkl'


def check_existing_file() -> bool:
    """
    Check if comments subset file exists and prompt the user.

    :return: True if can continue
    """
    if os.path.isfile(comments_subset_file):
        user_continue = input(f'{comments_subset_file} already exists. Do you want to continue? ([y]/n) ')
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
        with open(posts_subset_file, 'r', encoding='utf-8') as f:
            for line in tqdm(f, desc='Making post id list', total=LINES_SUBSET):
                match = id_pattern.search(line)
                if match:
                    post_id = match.group(1)
                    post_ids.add(post_id)
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
        for line in tqdm(f, desc=f'Processing comment lines... (Will write max {int(LINES_SUBSET/1_000_000)}M lines in total)', total=LINES_SUBSET / 0.1):  # Approximation
            count += 1
            parent_id = find_parent_post(line)
            if parent_id and parent_id in post_ids:
                with open(comments_subset_file, 'a', encoding='utf-8') as f_out:
                    f_out.write(str(count))
                count_lines += 1

            if count_lines % 100_000 == 0:
                print(
                    f'Wrote {count_lines:,} lines... ({count_lines / count * 100:.1f}% | {count_lines / LINES_SUBSET * 100:.1f}%)')

            if count_lines == LINES_SUBSET:
                break
        print(
            f'Finished. Wrote {count_lines:,} lines... ({count_lines / count * 100:.1f}% | {count_lines / LINES_SUBSET * 100:.1f}%)')


def make_subset_comments():
    """
    Main function to run all steps for creating the comments subset.
    """
    # Subset files paths
    os.makedirs('data/subset', exist_ok=True)

    if check_existing_file():
        post_ids = load_or_create_post_ids()
        create_comment_subset(post_ids)
    else:
        print('Skipping creating comment subset...')


# Run the process
if __name__ == "__main__":
    make_subset_comments()
