from classes.BaseCleaner import BaseCleaner
from datetime import datetime
import re
import hashlib
from general import capitalize_db_type


class SubredditRulesCleaner(BaseCleaner):
    def clean(self, line: dict) -> list[dict]:
        """
        Helper method to process a line for the subreddit_rules table. Unpacks the rule dictionary.

        :param line: Line to process.
        :return: list of dictionaries, each containing one rule
        """
        lines_rules_cleaned = []
        subreddit = line["subreddit"]

        for rule in line["rules"]:
            # Build a consistent string from rule content and subreddit
            rule_string = f'{subreddit}_{rule["priority"]}_{rule["short_name"]}_{rule["description"]}'
            rule_hash = hashlib.md5(rule_string.encode()).hexdigest()

            rule = {"rule_id": rule_hash, **rule, "subreddit": subreddit}  # Put rule_id first for readability
            lines_rules_cleaned.append(rule)

        return lines_rules_cleaned

class RemovedCleaner(BaseCleaner):
    def clean(self, line):
        if line['removal_reason'] is None and line['removed_by'] is None:
            return None
        return line

class WikiCleaner(BaseCleaner):
    def __init__(self, db_type):
        self.db_type = db_type

    def clean(self, line):
        try:
            dt = datetime.fromisoformat(line['revision_date'].replace("Z", "+00:00"))
        except Exception as e:
            print(f'[{capitalize_db_type(self.db_type)}] KEY ERROR! REVISION DATE: {e}')
            print(line)
            exit(1)

        line['revision_date'] = int(dt.timestamp())
        match = re.search(r"/r/([^/]+)", line['path'])
        if not match:
            return None
        line['subreddit'] = match.group(1)
        line['content'] = line['content'][:500]
        return line

class WikiRevisionCleaner(BaseCleaner):
    def clean(self, line):
        return line

class PostCleaner(BaseCleaner):
    def clean(self, line):
        line['edited'] = bool(int(line['edited']))
        return line

class CommentCleaner(BaseCleaner):
    def clean(self, line):
        line['edited'] = bool(int(line['edited']))
        return line

class CollapsedCommentCleaner(BaseCleaner):
    def clean(self, line):
        return line

class DistinguishedCommentCleaner(BaseCleaner):
    def clean(self, line):
        return line

class AuthorCleaner(BaseCleaner):
    def __init__(self, ignored_authors):
        self.ignored_authors = ignored_authors

    def clean(self, line):
        if line['author'].strip().lower() in self.ignored_authors:
            return None
        return line

class DistinguishedPostCleaner(BaseCleaner):
    def clean(self, line):
        if line['distinguished'] is None:
            return None
        return line

class SubredditCleaner(BaseCleaner):
    def clean(self, line):
        return line

class SubredditMetadataCleaner(BaseCleaner):
    def clean(self, line):
        return line

class SubredditCommentMediaCleaner(BaseCleaner):
    def clean(self, line):
        return line

class SubredditMediaCleaner(BaseCleaner):
    def clean(self, line):
        return line

class SubredditSettingsCleaner(BaseCleaner):
    def clean(self, line):
        return line

class SubredditPermissionsCleaner(BaseCleaner):
    def clean(self, line):
        return line
