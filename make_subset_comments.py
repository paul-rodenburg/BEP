import os
import sys
import ujson as json
from config import comments_file, posts_subset_file,comments_subset_file, LINES_SUBSET
import re
from tqdm import tqdm
import pickle

post_ids = set()
id_pattern = re.compile(r'"id"\s*:\s*"([\w-]+)"')
post_ids_path = 'cache/post_ids.pkl'
os.makedirs('cache', exist_ok=True)
if os.path.isfile(comments_subset_file):
    user_continue = input(f'{comments_subset_file} already exists. Do you want to continue? ([y]/n)')
    if user_continue.lower() == 'n':
        sys.exit(0)  # User doesn't want to continue so exit process


# Determine if list already exists
if os.path.isfile(post_ids_path):
    with open(post_ids_path, 'rb') as f:
        post_ids = pickle.load(f)
else:
    PRINT = True
    with open(posts_subset_file, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc='Making post id list', total=LINES_SUBSET):
            match = id_pattern.search(line)
            if match:
                post_id = match.group(1)
                post_ids.add(post_id)
                if PRINT:
                    print(line)
                    print(post_id)
                    PRINT = False
    with open(post_ids_path, 'wb') as f:
        pickle.dump(post_ids, f)



def find_parent_post(line):
    parent_id_pattern = re.compile(r'"parent_id"\s*:\s*"(\w+)"')
    match = parent_id_pattern.search(line)
    parent_id = match.group(1)
    if '_' in parent_id:
        parent_id = parent_id.split('_')[1]
    return parent_id


count_lines = 0
count = 0
lines_to_write = []

with open(comments_file, 'r', encoding='utf-8') as f:
    for line in tqdm(f, desc='Making comment subset', total=LINES_SUBSET/0.1):  # Total number is an approximation
        count += 1
        parent_id = find_parent_post(line)
        if parent_id in post_ids:
            lines_to_write.append(line)
            count_lines += 1
        if count % 1000000 == 0:
            print(f'Wrote {count_lines:,} lines... ({count_lines / count * 100:.1f}% | {count_lines/LINES_SUBSET*100:.1f}%)')
        if count_lines == LINES_SUBSET:
            break

print(f'Collected {count_lines} lines, writing to file...')

with open(comments_subset_file, 'w', encoding='utf-8') as f_out:
    f_out.writelines(lines_to_write)

print(f'Finished writing {count_lines} lines.')
