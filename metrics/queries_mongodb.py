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
        },
        "joins": {
            "query_type": "join",
            "queries": [
                {
                    "name": "join_posts_with_comments",
                    "query": lambda db: list(db["post"].aggregate([
                        {
                            "$lookup": {
                                "from": "comment",
                                "localField": "id",
                                "foreignField": "link_id",
                                "as": "comments"
                            }
                        },
                        {"$limit": 5},  # Optional: limit output for testing
                        {
                            "$project": {
                                "_id": 0,
                                "id": 1,
                                "title": 1,
                                "num_comments": {"$size": "$comments"},
                                "comments": {
                                    "$map": {
                                        "input": "$comments",
                                        "as": "c",
                                        "in": "$$c.body"
                                    }
                                }
                            }
                        }
                    ]))
                },
                {
                    "name": "join_posts_with_subreddits",
                    "query": lambda db: list(db["post"].aggregate([
                        {
                            "$lookup": {
                                "from": "subreddit",
                                "localField": "subreddit_id",
                                "foreignField": "name",
                                "as": "subreddit_info"
                            }
                        },
                        { "$unwind": "$subreddit_info" },
                        {
                            "$project": {
                                "_id": 0,
                                "id": 1,
                                "title": 1,
                                "display_name": "$subreddit_info.display_name"
                            }
                        }
                    ]))
                },
                {
                    "name": "join_comments_with_posts",
                    "query": lambda db: list(db["comment"].aggregate([
                        {
                            "$lookup": {
                                "from": "post",
                                "localField": "parent_id",
                                "foreignField": "id",
                                "as": "post_info"
                            }
                        },
                        { "$unwind": "$post_info" },
                        {
                            "$project": {
                                "_id": 0,
                                "comment_id": "$id",
                                "post_title": "$post_info.title"
                            }
                        }
                    ]))
                }
                ]
        },
        "advanced": {
            "query_type": "advanced",
            "queries": [
                {
                    "name": "advanced_top_posts_by_score",
                    "query": lambda db: list(db["post"].aggregate([
                        {"$sort": {"score": -1}},
                        {"$limit": 10},
                        {"$project": {"_id": 0, "title": 1, "score": 1}}
                    ]))
                },
                {
                    "name": "advanced_average_comments_per_post",
                    "query": lambda db: list(db["post"].aggregate([
                        {
                            "$group": {
                                "_id": None,
                                "avg_comments": {"$avg": "$num_comments"}
                            }
                        },
                        {"$project": {"_id": 0, "avg_comments": 1}}
                    ]))
                },
                {
                    "name": "advanced_active_subreddits_by_post_count",
                    "query": lambda db: list(db["post"].aggregate([
                        {
                            "$group": {
                                "_id": "$subreddit_id",
                                "post_count": {"$sum": 1}
                            }
                        },
                        {"$sort": {"post_count": -1}},
                        {"$limit": 10},
                        {
                            "$project": {
                                "_id": 0,
                                "subreddit_id": "$_id",
                                "post_count": 1
                            }
                        }
                    ]))
                }
            ]
        },
        "nested": {
            "query_type": "nested",
            "queries": [
                {
                    "name": "nested_top_commenters_on_top_post",
                    "query": lambda db: (
                        lambda top_post_id: list(
                            db["comment"].find(
                                {"link_id": top_post_id},
                                {"id": 0, "author_fullname": 1}
                            )
                        )
                    )(db["post"].find_one(sort=[("score", -1)])["id"])
                },
                {
                    "name": "nested_posts_by_most_active_author",
                    "query": lambda db: (
                        lambda top_author: list(
                            db["post"].find(
                                {"author_fullname": top_author},
                                {"_id": 0, "id": 1, "title": 1}
                            )
                        )
                    )(db["post"].aggregate([
                        {
                            "$group": {
                                "_id": "$author_fullname",
                                "count": {"$sum": 1}
                            }
                        },
                        {"$sort": {"count": -1}},
                        {"$limit": 1}
                    ]).next()["_id"])
                },
                {
                    "name": "nested_subreddits_with_high_avg_score",
                    "query": lambda db: list(
                        db["post"].aggregate([
                            {
                                "$group": {
                                    "_id": "$subreddit_id",
                                    "avg_score": {"$avg": "$score"}
                                }
                            },
                            {"$match": {"avg_score": {"$gt": 1000}}},
                            {
                                "$project": {
                                    "_id": 0,
                                    "subreddit_id": "$_id"
                                }
                            }
                        ])
                    )
                }
            ]
        }
    }