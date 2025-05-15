def get_queries():
    return {
        "simple": {
            "query_type": "simple",
            "queries": [
                {
                    "name": "simple_select_authors",
                    "query": lambda col: list(col.find({}, { "author_fullname": 1, "author": 1 })),
                    "collection": "post"
                },
                {
                    "name": "simple_count_posts",
                    "query": lambda col: col.count_documents({}),
                    "collection": "post"
                },
                {
                    "name": "simple_unique_subreddits",
                    "query": lambda col: col.distinct("subreddit_id"),
                    "collection": "post"
                }
            ]
        }
    }