# Config
LINES_SUBSET = 40_000_000  # Number of lines/comments/posts in the subset (_ for better readability)

posts_subset_base_name = 'data/subset/subset_posts'
comments_subset_file = 'data/subset/subset_comments'
rules_subset_file = 'data/subset/subset_rules'
wikis_subset_file = 'data/subset/subset_wikis'
subreddits_subset_file = 'data/subset/subset_subreddits'

# Content files
comments_file = 'data/comments/RC_2025-01/RC_2025-01'
posts_2025_1_file = 'data/submissions/RS_2025-01/RS_2025-01'
posts_2025_2_file = 'data/submissions/RS_2025-02/RS_2025-02'
posts_2024_12_file = 'data/submissions/RS_2024-12/RS_2024-12'
posts_2024_11_file = 'data/submissions/RS_2024-11/RS_2024-11'
posts_2024_10_file = 'data/submissions/RS_2024-10/RS_2024-10'
posts_2024_09_file = 'data/submissions/RS_2024-10/RS_2024-10'

# Subreddit files
subreddit_rules_file = 'data/subreddits/subreddit_rules_2025-01/subreddit_rules_2025-01'
subreddit_wiki_file = 'data/subreddits/subreddit_wikis_2025-01/subreddit_wikis_2025-01'
subreddits_file = 'data/subreddits/subreddits_2025-01/subreddits_2025-01'

dates_subsets = ['2025-1', '2024-12', '2025-2', '2024-11', '2024-10', '2024-09']  # In order of priority
dates_to_original_file = {'2025-1': posts_2025_1_file,
                          '2024-12': posts_2024_12_file,
                          '2025-2': posts_2025_2_file,
                          '2024-11': posts_2024_11_file,
                          '2024-10': posts_2024_10_file,
                          '2024-09': posts_2024_09_file}

# SQL database
subset_files_tables = {posts_subset_base_name: ['post', 'author', 'banned'],
                       comments_subset_file: ['comment'],
                       rules_subset_file: ['subreddit_rules'],
                       wikis_subset_file: ['wiki', 'revision_wiki'],
                       subreddits_subset_file: ['subreddit', 'subreddit_metadata', 'subreddit_settings',
                                                'subreddit_media', 'subreddit_permissions', 'subreddit_comment_media']}


data_files = [posts_2025_1_file, comments_file, subreddits_file, subreddit_rules_file, subreddit_wiki_file]

data_files_tables = {posts_2025_1_file: ['post', 'author', 'banned'],
                     comments_file: ['comment'],
                     subreddits_file: ['subreddit_rules'],
                     subreddit_rules_file: ['wiki', 'revision_wiki'],
                     subreddit_wiki_file: ['subreddit', 'subreddit_metadata', 'subreddit_settings',
                                                'subreddit_media', 'subreddit_permissions', 'subreddit_comment_media']}

subset_to_original = {comments_subset_file: comments_file,
                      rules_subset_file: subreddit_rules_file,
                      wikis_subset_file: subreddit_wiki_file,
                      subreddits_subset_file: subreddits_file}
