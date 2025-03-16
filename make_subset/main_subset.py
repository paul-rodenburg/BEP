from make_subset_posts import make_post_subset
from make_subset_comments import make_subset_comments
from subreddit_subset import make_subset_subreddits
import os
import sys

if __name__ == "__main__":
    # Update working directory
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)

    # Run functions to make subsets
    make_post_subset()
    make_subset_comments()
    make_subset_subreddits()