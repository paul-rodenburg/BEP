import os
import sys

from general import get_line_count_file
from pygments.lexers import q
from tqdm import tqdm
from config import comments_file, posts_subset_file,comments_subset_file, LINES_SUBSET, subreddit_rules_file, rules_subset_file, subreddit_wiki_file, subreddits_file
import re
import pickle

from make_subset_comments import user_continue

cache_file = 'cache/subreddit_names.pkl'
os.makedirs('cache', exist_ok=True)

if os.path.isfile(rules_subset_file):
    user_continue = input(f'The file {rules_subset_file.split("/")[-1]} already exists. Continue? ([y]/n)')
    if user_continue.lower() == 'n':
        sys.exit(0)


# Make subreddit cache file
if os.path.isfile(cache_file):
    with open(cache_file, 'rb') as f:
        subreddits = pickle.load(f)
else:
    subreddits = set()
    id_pattern = re.compile(r'"subreddit_name_prefixed"\s*:\s*"([^"]+)"')
    with open(posts_subset_file, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc='Making subreddit set', total=LINES_SUBSET):
            match = id_pattern.search(line)
            subreddit = match.group(1)
            subreddit = subreddit[2:]  # Remove r/ prefix
            subreddits.add(subreddit)
    with open(cache_file, 'wb') as f:
        pickle.dump(subreddits, f)


# Make reddit subsets
list_subreddits = {subreddit_rules_file: {'file': rules_subset_file, 'regex': r'"subreddit"\s*:\s*"([^"]+)"'},
                   subreddit_wiki_file: {'file': rules_subset_file, 'regex': r'"subreddit"\s*:\s*"([^"]+)"'},
                   subreddits_file: {'file': rules_subset_file, 'regex': r'"subreddit"\s*:\s*"([^"]+)"'}}
for file in list_subreddits
    NUMBER_LINES_RULES = get_line_count_file(subreddit_rules_file)
    subreddit_pattern = re.compile(file['regex'])
    with open(file, 'r', encoding='utf-8') as f:
        for line in tqdm(f, desc=f'Making {file.split("/")[-1]} subset', total=NUMBER_LINES_RULES):
            subreddit = subreddit_pattern.search(line).group(1)
            if subreddit in subreddits:
                with open(file['file'], 'a', encoding='utf-8') as f:
                    f.write(line)
