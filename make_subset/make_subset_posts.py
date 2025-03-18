import os
import ujson as json
from tqdm import tqdm
from config import *
import sys

def check_existing_file() -> bool:
    """
    Check if posts subset file exists and prompt the user.

    :return: True if can continue
    """
    if os.path.isfile(posts_subset_file):
        user_continue = input(f'{posts_subset_file} already exists. Do you want to continue? ([y]/n) ')
        if user_continue.lower() == 'n':
            return False
        else:
            os.remove(posts_subset_file)
            return True
    return True


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


def create_post_subset_file():
    # Find lines with nested JSON objects
    count = 0
    count_lines = 0
    with open(posts_2025_file, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc=f'Processing post lines... (Will write max {int(LINES_SUBSET/1_000_000)}M lines in total)', total=int(LINES_SUBSET/0.2)):
            count += 1
            line_json = json.loads(line)  # Parse JSON
            nested_keys = find_non_empty_nested_keys(line_json)  # Find non-empty nested JSON

            if nested_keys:  # Only print if nested objects have length > 0
                count_lines += 1
                with open(posts_subset_file, 'a', encoding='utf-8') as f_out:
                    f_out.write(str(count))
            if count_lines % 100_000 == 0:
                print(f'Wrote {count_lines:,} lines...({count_lines/count*100:.1f}% | {count_lines/LINES_SUBSET*100:.1f}%)')
            if count_lines == LINES_SUBSET:
                break
        print(f'Finished. Wrote {count_lines:,} lines...({count_lines / count * 100:.1f}% | {count_lines / LINES_SUBSET * 100:.1f}%)')


def make_post_subset():
    # Subset files paths
    os.makedirs('data/subset', exist_ok=True)

    if check_existing_file():
        create_post_subset_file()
    else:
        print('Skipping creating post subset...')


if __name__ == '__main__':
    make_post_subset()