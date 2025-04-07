from make_subset_posts import make_post_subset
from make_subset_comments import make_subset_comments
from subreddit_subset import make_subset_subreddits
import os

if __name__ == "__main__":
    input_cont = input('Making subsets is not supported (not a fair comparison). Do you want to continue? (y/n)')
    if input_cont.strip().lower() == 'n':
        exit(0)
    # Update working directory
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)

    # Make folders
    os.makedirs('cache', exist_ok=True)
    os.makedirs('data/subset', exist_ok=True)

    # Run functions to make subsets
    make_post_subset()
    make_subset_comments()
    make_subset_subreddits()