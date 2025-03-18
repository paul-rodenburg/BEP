# Config
LINES_SUBSET = 20_000_000  # Number of lines/comments/posts in the subset (_ for better readability)

posts_subset_file = 'data/subset/subset_posts'
comments_subset_file = 'data/subset/subset_comments'
rules_subset_file = 'data/subset/subset_rules'
wikis_subset_file = 'data/subset/subset_wikis'
subreddits_subset_file = 'data/subset/subset_subreddits'

# Content files
comments_file = 'data/comments/RC_2025-01/RC_2025-01'
posts_2025_file = 'data/submissions/RS_2025-01/RS_2025-01'
posts_2024_file = 'data/2024/reddit/submissions/RS_2024-12/RS_2024-12'

# Subreddit files
subreddit_rules_file = 'data/other/reddit/subreddits/subreddit_rules_2025-01/subreddit_rules_2025-01'
subreddit_wiki_file = 'data/other/reddit/subreddits/subreddit_wikis_2025-01/subreddit_wikis_2025-01'
subreddits_file = 'data/other/reddit/subreddits/subreddits_2025-01/subreddits_2025-01'


# SQL database
subset_files_tables = {posts_subset_file: ['post', 'author', 'banned', 'removed'],
                       comments_subset_file: ['comment'],
                       rules_subset_file: ['subreddit_rules'],
                       wikis_subset_file: ['wiki', 'revision_wiki'],
                       subreddits_subset_file: ['subreddit', 'subreddit_metadata', 'subreddit_settings',
                                                'subreddit_media', 'subreddit_permissions', 'subreddit_comment_media']}

subset_to_original = {posts_subset_file: posts_2025_file,
                      comments_subset_file: comments_file,
                      rules_subset_file: subreddit_rules_file,
                      wikis_subset_file: subreddit_wiki_file,
                      subreddits_subset_file: subreddits_file}
