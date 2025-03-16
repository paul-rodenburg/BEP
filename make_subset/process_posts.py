import json
from make_subset_posts import find_non_empty_nested_keys
from data_to_sql import clean_line
from config import posts_2025_file, subset_files_tables, posts_subset_file

tables = subset_files_tables[posts_subset_file]
with open(posts_2025_file, 'r', encoding='utf-8') as f:
    for line in f:
        nested_keys = find_non_empty_nested_keys(json.loads(line))
        if nested_keys:
            for table in tables:
                cleaned_line = clean_line(line, table)
                cleaned_line = json.dumps(cleaned_line)
                with open(f'../data/subset/table_{table}', 'a', encoding='utf-8') as f:
                    f.write(cleaned_line)