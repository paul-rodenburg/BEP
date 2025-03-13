import os
import ujson as json  # Faster JSON parsing
from tqdm import tqdm
from config import *


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

if __name__ == '__main__':
    # Subset files paths
    os.makedirs('data/subset', exist_ok=True)

    if os.path.isfile(posts_subset_file):
        # Ask user to delete file to regenerate it
        delete_file = input('Do you want to delete the posts 2025 subset file? ([y]/n): ')
        if delete_file.lower() != 'n':  # Default option is to delete the file (y)
            os.remove(posts_subset_file)
            # Make empty file (the process that creates the subset will only append lines; this is only possible when a file already exists)
            with open(posts_subset_file, 'w', encoding='utf-8') as f:
                pass  # Creates an empty file


    # Find lines with nested JSON objects
    count = 0
    count_lines = 0
    with open(posts_2025_file, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc=f'Reading lines... (Will write {LINES_SUBSET:,} lines in total)'):
            count += 1
            line_json = json.loads(line)  # Parse JSON
            nested_keys = find_non_empty_nested_keys(line_json)  # Find non-empty nested JSON

            if nested_keys:  # Only print if nested objects have length > 0
                count_lines += 1
                with open(posts_subset_file, 'a', encoding='utf-8') as f:
                    f.write(line)
            if count % 100000 == 0:
                print(f'Wrote {count_lines:,} lines...({count_lines/count*100:.1f}% | {count_lines/LINES_SUBSET*100:.1f}%)')
            if count_lines == LINES_SUBSET:
                break


