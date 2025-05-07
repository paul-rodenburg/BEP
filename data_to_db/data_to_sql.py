from typing import Any, Generator
from classes.DBType import DBTypes, DBType
from classes.logger import Logger
import pandas as pd
import orjson as json
import os
import math
from itertools import chain
from pandas import DataFrame
from sqlalchemy import text, Engine, Connection
from tqdm import tqdm
from general import get_tables_database, write_json, update_summary_log
from general import load_json_cached as load_json
from line_counts import get_line_count_file
import sys
import time
from classes.cleaners import *
from classes.BaseCleaner import BaseCleaner
from datetime import datetime

progress_bar = None
clean_errors = 0
maximum_rows_database = 0
MAX_MYSQL_TEXT_LENGTH = 65_500 # The actual max length is 65,535, but we keep some safety margin

# Load the schema in memory since it improved performance, reading the JSON many times takes time
schema_global = None


def get_primary_key(table_name, schema_json_file="schemas/db_schema.json"):
    global schema_global
    """
    Gets primary key columns of a table.

    :param table_name: Name of the table.
    :param schema_json_file: Path to the schema json file.

    :return: List of primary key columns.
    """
    schema = load_json(schema_json_file)
    return schema.get(table_name, {}).get("primary_keys", [])

def unnest(lst):
    """
    Unnests a list, however, if a list is not nested, it will not unnest it to a list of single characters.

    :param lst: List to unnest

    :return: Unnested list
    """
    if all(not isinstance(i, list) for i in lst):
        return lst  # Return as-is if it's already flat
    return list(chain.from_iterable(lst))


def get_table_columns(json_schema_path, table_name) -> list:
    """
    Gets the table columns from a sqlite database file.

    :param json_schema_path: Path to the JSON schema file.
    :param table_name: Name of the table to extract columns from.

    :return: List of table names in the sqlite database file.
    """
    schema = load_json(json_schema_path)
    columns = list(schema[table_name]['columns'].keys())
    return columns


def should_skip(line: dict|list[dict], primary_keys: list) -> bool:
    """
    Determines whether the line should be skipped based on the primary keys and other values.
    If any primary key value is None, the line is skipped. If all other values are None, the line is also skipped.

    :param line: Line to check.
    :param primary_keys: Primary keys to check.

    :return: True if the line should be skipped, False otherwise.

    """
    # Check if any primary key value is None
    if isinstance(line, list):
        line = line[0]
    if any(line.get(key) is None for key in primary_keys):
        return True

    # Get all other keys not in primary_keys
    other_keys = [k for k in line.keys() if k not in primary_keys]

    # If all other values are None
    if all(line.get(k) is None for k in other_keys):
        return True
    return False

def get_cleaner(table: str, db_type: DBType, ignored_author_names: set) -> BaseCleaner:
    """
    Returns the cleaner for the given table.

    :param table: Table name
    :param db_type: database type
    :param ignored_author_names: author names to ignore
    :return: cleaner for the given table.
    """
    cleaners = {
        'post': PostCleaner(),
        'distinguished_post': DistinguishedPostCleaner(),
        'author': AuthorCleaner(ignored_author_names),
        'subreddit': SubredditCleaner(),
        'subreddit_metadata': SubredditMetadataCleaner(),
        'subreddit_settings': SubredditSettingsCleaner(),
        'subreddit_media': SubredditMediaCleaner(),
        'subreddit_permissions': SubredditPermissionsCleaner(),
        'subreddit_comment_media': SubredditCommentMediaCleaner(),
        'subreddit_rules': SubredditRulesCleaner(),
        'removed': RemovedCleaner(),
        'comment': CommentCleaner(),
        'collapsed_comment': CollapsedCommentCleaner(),
        'distinguished_comment': DistinguishedCommentCleaner(),
        'wiki': WikiCleaner(db_type),
        'revision_wiki': WikiRevisionCleaner(),
    }
    return cleaners.get(table, BaseCleaner())


def clean_line(line_input: str, tables: list, table_columns: dict, ignored_author_names: set, db_type: DBType) -> dict[str, list[dict]]|None:
    """
    Gets a line and cleans it for all the tables.

    :param line_input: Line to clean
    :param tables: tables for the line
    :param table_columns: columns for the tables
    :param ignored_author_names: author names to ignore
    :param db_type: database type
    :return: A dict with as a key the table name and value the cleaned line for that table.
    If there is an error with the line, it returns None.
    """
    global clean_errors

    try:
        line_input = json.loads(line_input)
    except:
        clean_errors += 1
        return None

    cleaned_data = dict()
    for table in tables:
        cleaned_data[table] = None

    for table in tables:
        line = line_input
        items_to_keep = table_columns[table]

        cleaner = get_cleaner(table, db_type, ignored_author_names)
        line = cleaner.clean(line)
        if line is None:
            continue

        # If the value of the primary key(s) is/are null or all other values are null, then skip this line
        if should_skip(line, get_primary_key(table)):
            continue
        if not isinstance(line, list):
            line = [line]

        cleaned_lines = []
        for l in line:
            l = {key: l[key] for key in items_to_keep if key in l}
            cleaned_lines.append(l)

        cleaned_data[table] = cleaned_lines

    return cleaned_data

seen_authors = set()

def process_cleaned_lines(cleaned_lines_dct) -> dict[str, pd.DataFrame]:
    global seen_authors

    """
    Converts cleaned line entries to DataFrames, optionally ensuring globally unique rows based on primary keys.

    :param cleaned_lines_dct: cleaned lines dictionary
    :return: A dict with the table name as key and a DataFrame (deduplicated if authors) as value.
    """
    for table_name, data in cleaned_lines_dct.items():
        if not data:
            cleaned_lines_dct[table_name] = None
            continue

        primary_key_column = get_primary_key(table_name)
        if not primary_key_column:
            cleaned_lines_dct[table_name] = None
            continue

        # Check if users are already added
        if table_name == 'author':
            unique_rows_author = []
            for d in data:
                author_fullname = d['author_fullname']
                if author_fullname in seen_authors:
                    continue
                seen_authors.add(author_fullname)
                unique_rows_author.append(d)

            cleaned_lines_dct['author'] = unique_rows_author
            del unique_rows_author  # Clean the list to clean memory
            if len(cleaned_lines_dct['author']) == 0:
                cleaned_lines_dct['author'] = None


        df = pd.DataFrame(cleaned_lines_dct[table_name])

        # Clean up strange data types and null characters
        df = df.map(lambda x: str(x) if isinstance(x, (list, dict)) else x)
        df = df.map(lambda x: x.replace("\x00", "") if isinstance(x, str) else x)

        cleaned_lines_dct[table_name] = df
    return cleaned_lines_dct



sql_count = 0

def process_table(data_file: str, tables: list, engine: Engine,
                  table_columns: dict, ignored_author_names: set, chunk_size: int, db_type: DBType):
    """
    Processes tables, so writing the data to a database.

    :param data_file: Path to data file
    :param tables: tables to process
    :param engine: database connection
    :param table_columns: dictionary containing tables names as keys and the value are the column names corresponding to the tables
    :param ignored_author_names: author names to ignore.
    :param chunk_size: Number of lines to read at a time
    :param db_type: database type
    """
    global sql_count

    added_count = 0
    for chunk_data in extract_lines(data_file, tables, table_columns, ignored_author_names, db_type, chunk_size):
        for table_name, data in chunk_data.items():
            if data is not None and not data.empty:
                write_to_db(data, table_name, engine, len(chunk_data), chunk_size=chunk_size, db_type=db_type)
                added_count += 1
    sql_count = 0  # Reset count for the progress bar
    if added_count == 0:
        print(f'[{db_type.to_string_capitalized()}] Error! All chunks of {tables} were empty')


def extract_lines(data_file: str, tables: list, table_columns: dict, ignored_author_names: set, db_type: DBType, chunk_size: int) -> Generator[
    dict[str, DataFrame], Any, None]:
    """
    Processes lines from the Reddit data file.

    :param data_file: Path to the Reddit data file
    :param tables: Tables to process
    :param table_columns: Dictionary containing tables names as keys and the value are the column names
    corresponding to the tables
    :param ignored_author_names: Author names to ignore
    :param db_type: The database type
    :param chunk_size: Number of lines to read at a time

    :return: A dict with as a key the table name and value the cleaned lines for that table in pandas DataFrame
    """
    global progress_bar
    lines_clean = {}
    for table_name in tables:
        lines_clean[table_name] = []

    start_time = datetime.now()
    progress_bar_total = min(get_line_count_file(data_file), maximum_rows_database)
    progress_bar = tqdm(total=progress_bar_total, desc=f"[{db_type.to_string_capitalized()}] Processing {len(tables)} table(s): {tables} (from {data_file.split('/')[-1]})")

    lines_cleaned_count = 0
    with open(data_file, 'r', encoding='utf-8') as f_data:

        for line in f_data:
            cleaned_data = clean_line(line, tables, table_columns, ignored_author_names, db_type)
            for table_name, lines_cleaned in cleaned_data.items():
                if lines_cleaned is not None:
                    lines_clean[table_name].extend(lines_cleaned)

            lines_cleaned_count += 1
            progress_bar.update(1)

            if lines_cleaned_count % chunk_size == 0 and lines_cleaned_count > 0:
                yield process_cleaned_lines(lines_clean)
                # Clean the dict for the next iteration
                lines_clean = dict()
                for table_name in tables:
                    lines_clean[table_name] = []
            if lines_cleaned_count >= maximum_rows_database:
                break

    # Write progress bar results to log a file
    print(str(progress_bar))

    progress_bar.close()
    if lines_clean:
        yield process_cleaned_lines(lines_clean)

    # Update log summary
    end_time = datetime.now()
    update_summary_log(db_type=db_type, data_file=data_file,
                       start_time=start_time, end_time=end_time,
                       line_count=lines_cleaned_count, total_lines=progress_bar_total,
                       tables=tables, chunk_size=chunk_size,
                       sql_writes=sql_count)


def write_to_db(df: pd.DataFrame, table: str, conn: Engine, len_tables: int, db_type: DBType, chunk_size: int=10_000):
    """
    Write dataframe to the database.

    :param df: Pandas DataFrame
    :param table: table name
    :param conn: database connection
    :param len_tables: number of tables that are currently being processed (only used to display in tqdm progress bar)
    :param db_type: database type, either sqlite, mysql, or PostgreSQL
    :param chunk_size: number of lines to read at a time
    """
    global sql_count, progress_bar
    try:
        df.to_sql(table, conn, if_exists="append", index=False, chunksize=5000)
    except Exception as e:
        df.to_csv('error.csv', index=False)
        print(f"\n[{db_type.to_string_capitalized()}] Error writing to database: {e}. df written to error.csv.")
        exit(1)
    sql_count += 1
    progress_bar.set_postfix_str(f'[{sql_count:,}/{math.ceil(progress_bar.total / chunk_size * len_tables):,} SQL writes]')


def is_file_tables_added_db(data_file, tables, db_info_file) -> list:
    """
    Gets the table names of the table names that are not (fully) processed, so not (completely) added to the database.

    :param data_file: Path to the reddit data file
    :param tables: list of table names to check
    :param db_info_file: the path to the database info file

    :return: List of table names that need to be added to the database
    """
    if os.path.isfile(db_info_file):
        data = load_json(db_info_file)


        for obj in data:
            if obj['file'] == data_file:
                # Find tables that are in `tables` but not in `success_tables`
                missing_tables = list(set(tables) - set(obj['success_tables']))
                return missing_tables

    # If a file doesn't exist or the file not found, return all tables
    return tables


def add_file_table_db_info(data_file, tables, db_info_file):
    """
    Adds content to the success_tables in the database-specific JSON file.

    :param data_file: Path to the reddit data file
    :param tables: table names to add to the success_tables field in the db_info_file
    :param db_info_file: path to the JSON db info file
    """
    if not os.path.isfile(db_info_file):
        write_json([], db_info_file)

    data = load_json(db_info_file)

    file_entry = next((obj for obj in data if obj['file'] == data_file), None)

    if file_entry:
        if not all(item in file_entry['success_tables'] for item in tables):
            file_entry['success_tables'].append(tables)
            file_entry['success_tables'] = unnest(file_entry['success_tables'])
            file_entry['success_tables'] = list(set(file_entry['success_tables']))
    else:
        data.append({'file': data_file, 'success_tables': tables})

    write_json(data, db_info_file)


def get_tables_to_skip(json_data) -> set:
    """
    Gets the table names of tables that already do not include any duplicates

    :param json_data: data of the database-specific info
    """
    tables_to_skip = set()
    for entry in json_data:
        if "duplicates_removed" in entry:
            tables_to_skip.update(entry["duplicates_removed"])
    return tables_to_skip

def update_json_with_table_duplicates(json_file, table_name, data_file):
    """
    Adds the table names of tables where duplicates are removed to the database-specific info JSON file.

    :param json_file: Path to the JSON file with database-specific info
    :param table_name: table name of table where duplicates are removed
    :param data_file: path to the file with reddit data

    """
    json_data = load_json(json_file)
    for entry in json_data:
        if "success_tables" in entry and table_name in entry["success_tables"] and data_file in entry["file"]:
            if "duplicates_removed" not in entry:
                entry["duplicates_removed"] = []
            if table_name not in entry["duplicates_removed"]:
                entry["duplicates_removed"].append(table_name)
    write_json(json_data, json_file)

def clean_json_duplicates(json_file):
    """
    Cleans the JSON duplicates field,
    ensures that a table name is only in the duplicates_removed field if in success_tables.

    :param json_file: The path to the JSON with database-specific info
    """
    json_data = load_json(json_file)
    for entry in json_data:
        if "duplicates_removed" in entry and "success_tables" in entry:
            entry["duplicates_removed"] = [table for table in entry["duplicates_removed"] if table in entry["success_tables"]]
    write_json(json_data, json_file)

delete_all = False

def delete_table_db(table_name: str, engine: Engine, db_type: DBType):
    """
    If a table exists, asks the user to delete it. If the table does not exist, noting is done.

    :param table_name: Name of the table to delete
    :param engine: Connection to the database :param db_type:  type, either sqlite, mysql, or postgresql
    :param db_type: Database type, either sqlite, mysql, or PostgreSQL

    :raises ValueError: If the connection type is not supported
    """
    # Global variables
    global delete_all

    with engine.connect() as conn:
        if table_exists(conn, table_name, db_type):
            if delete_all:
                delete_confirm = 'y'
            else:
                delete_confirm = input(
                    f'[{db_type.to_string_capitalized()}] Table {table_name} already exists. Delete anyway? (y/n) (or yy to delete all)')
            if delete_confirm.lower().strip() != 'y':
                if delete_confirm.lower().strip() == 'yy':
                    delete_all = True
                    print(f'[{db_type.to_string_capitalized()}] Deleting all...')
                else:
                    return True
        else:  # Table does not exist
            pass

    match db_type:
        case DBTypes.SQLITE:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f'[{db_type.to_string_capitalized()}] Deleted table {table_name}')
        case DBTypes.MYSQL:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f'[{db_type.to_string_capitalized()}] Deleted table {table_name}')
        case DBTypes.POSTGRESQL:
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            print(f'[{db_type.to_string_capitalized()}] Deleted table {table_name}')
        case _:
            raise ValueError(f'[{db_type.to_string_capitalized()}] Unknown database type: {db_type}')


def table_exists(connection: Connection, table_name: str, db_type: DBType):
    """
    Checks if a table exists in the database.

    :param connection: Connection to the database
    :param table_name: Name of the table
    :param db_type: Database type

    :raises ValueError: If the database type is not supported

    :return: True if the table exists, False otherwise
    """
    # For every database type there is a different query to get the tables that are in that database,
    # so based on the db type execute a specific query to get the existing tables
    if db_type.is_type(DBTypes.MYSQL):
        query = text(f"SHOW TABLES LIKE :table")
        result = connection.execute(query, {'table': table_name}).fetchone()
        return result is not None

    elif db_type.is_type(DBTypes.POSTGRESQL):
        query = text(f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = :table
            )
        """)
        result = connection.execute(query, {'table': table_name}).fetchone()
        return result[0]

    elif db_type.is_type(DBTypes.SQLITE):
        query = text(f"SELECT name FROM sqlite_master WHERE type='table' AND name=:table")
        result = connection.execute(query, {'table': table_name}).fetchone()
        return result is not None

    else:
        raise ValueError(f'[{db_type.to_string_capitalized()}] Unknown database type: {db_type}')


def set_index(engine: Engine, table_name: str, db_type: DBType):
    """
    Sets the index of a database table, ensuring efficient lookups.
    If the index already exists, it will be recreated.

    :param engine: Database engine
    :param table_name: the name of the table you want to set the index for
    :param db_type: the type of the database, either sqlite, mysql, or postgresql

    :raises ValueError: If the connection type is not supported
    """
    pms = get_primary_key(table_name)

    print(f"[{db_type.to_string_capitalized()}] Setting index for table '{table_name}' and columns {pms}...")
    
    # Set the index for the primary key columns
    for pm in pms:
        if db_type.is_type(DBTypes.SQLITE):
            # SQLite doesn't use 'connect()', just use the provided 'conn'
            with engine.connect() as conn:
                # Drop the index if it already exists
                conn.execute(text(f"DROP INDEX IF EXISTS index_{pm}"))
                # Create the index
                conn.execute(text(f"CREATE INDEX index_{pm} ON {table_name} ({pm})"))

        elif db_type.is_type(DBTypes.MYSQL):
            with engine.connect() as conn:
                # Check if the table exists
                if not table_exists(conn, table_name, db_type):
                    print(f"Table '{table_name}' does not exist. Skipping index creation.")
                    return

                # Check if the index exists
                index_check_query = text(f"""
                    SELECT 1 FROM information_schema.statistics
                    WHERE table_name = :table_name AND index_name = :index_name
                """)
                result = conn.execute(index_check_query,
                                      {'table_name': table_name, 'index_name': f'index_{pm}'}).fetchone()

                # Drop the index if it exists
                if result:
                    drop_index_query = text(f"DROP INDEX index_{pm} ON {table_name}")
                    conn.execute(drop_index_query)

                # Check if the column is of type TEXT or BLOB
                column_type_query = text(f"""
                    SELECT COLUMN_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = :table_name AND COLUMN_NAME = :column_name
                """)
                column_type = conn.execute(column_type_query,
                                           {'table_name': table_name, 'column_name': pm}).fetchone()

                # If the column is TEXT or BLOB, we need to specify the length for the index
                if column_type and ('text' in column_type[0].lower() or 'blob' in column_type[0].lower()):
                    create_index_query = text(f"CREATE INDEX index_{pm} ON {table_name} ({pm}(255))")
                else:
                    create_index_query = text(f"CREATE INDEX index_{pm} ON {table_name} ({pm})")

                # Create the index
                conn.execute(create_index_query)
                conn.commit()

        elif db_type.is_type(DBTypes.POSTGRESQL):
            with engine.connect() as conn:
                # Drop the index if it already exists
                conn.execute(text(f"DROP INDEX IF EXISTS index_{pm}"))
                # Create the index
                conn.execute(text(f"CREATE INDEX index_{pm} ON {table_name} ({pm})"))
                conn.commit()

        else:
            raise ValueError(f'[{db_type.to_string_capitalized()}] Unknown database type: {db_type}')

def get_file_from_table_name(table_name: str) -> str|None:
    """
    Gets the data file path corresponding to the given table name.
    :param table_name: Name of table in the database to find the data file for
    :return: the path to the data file corresponding to the given table name.
    """
    config_data_tables = load_json('config.json')['data_files_tables']
    for k, v in config_data_tables.items():
        if table_name in v['sql']:
            return k

    # Return None if the table name is not found in the config.json file
    return None


def generate_create_table_statement(table_name: str, schema_json_file: str, db_type: DBType) -> str:
    """
    Makes the CREATE TABLE statements from the JSON schema file.

    :param schema_json_file: JSON schema file
    :param table_name: Name of the table
    :param db_type: Type of db (sqlite, postgreSQL, or mysql)

    :return: CREATE TABLE statement
    """
    # Load schema and check if passed table_name exist in the schema, if not raise an ValueError and exit program
    schema = load_json(schema_json_file)
    if table_name not in schema:
        raise ValueError(f"Table '{table_name}' not found in the schema.")

    table = schema[table_name]
    # MySQL has a shorter character length for TEXT than other databases, so change it to LONGTEXT if needed
    if db_type.is_type(DBTypes.MYSQL):
        data_file = get_file_from_table_name(table_name)
        character_lengths_data = load_json('character_lengths.json')[data_file]
        for col_name, col_type in table["columns"].items():
            if col_name in character_lengths_data:
                length = character_lengths_data[col_name]
            else:
                length = MAX_MYSQL_TEXT_LENGTH
            if length >= MAX_MYSQL_TEXT_LENGTH:
                table["columns"][col_name] = 'LONGTEXT'

    columns = table["columns"]
    primary_keys = table.get("primary_keys", [])

    lines = []
    
    # PostgreSQL has a different quotation mark for the table statement than the other database types,
    # so set the right quotation mark according to the current database type
    if db_type.is_type(DBTypes.SQLITE) or db_type.is_type(DBTypes.MYSQL):
        quotation_mark_table_statements = '`'
    elif db_type.is_type(DBTypes.POSTGRESQL):
        quotation_mark_table_statements = '"'
    else:
        raise ValueError(f'[{db_type.to_string_capitalized()}] Unsupported database type: {db_type}')

    for col_name, col_type in columns.items():

        # If the database is mysql and the current column is the only primary key, then mysql requires
        # that we set a maximum length of the primary keys (always type varchar).
        # Since the primary key value is always relatively short, we can pick 255 as the max length
        # For the values of non-primary keys; this is not necessary
        if db_type.is_type(DBTypes.MYSQL) and col_name in primary_keys and len(primary_keys) == 1 and col_type.lower() == 'text':
            col_type = 'VARCHAR(255)'

        line = f'  {quotation_mark_table_statements}{col_name}{quotation_mark_table_statements} {col_type}'

        # Don't add PRIMARY KEY here if there are multiple keys
        if isinstance(primary_keys, list) and len(primary_keys) == 1 and col_name in primary_keys:
            line += " PRIMARY KEY"
        lines.append(line)

    # Add a composite PRIMARY KEY constraint if needed
    if isinstance(primary_keys, list) and len(primary_keys) > 1:
        primary_keys_alt = [f'{quotation_mark_table_statements}{pk}{quotation_mark_table_statements}' for pk in primary_keys]
        pks_text = ", ".join(primary_keys_alt)
        pk_line = f'  PRIMARY KEY ({pks_text})'
        lines.append(pk_line)

    column_definitions = ",\n".join(lines)
    create_stmt = f'CREATE TABLE {quotation_mark_table_statements}{table_name}{quotation_mark_table_statements} (\n{column_definitions}\n);'

    return create_stmt

def create_tables_from_sql(engine: Engine, db_type: DBType, schema_json_file:str ='schemas/db_schema.json'):
    """
    Creates tables in the database from the provided JSON schema file,
    only if they don't already exist.

    :param engine: SQLAlchemy engine or sqlite3 connection.
    :param schema_json_file: Path to the schema JSON file
    :param db_type: type of db (sqlite, PostgreSQL, or mysql)
    """
    # Get db type (based on the db connection) and load the db schema
    schema = load_json(schema_json_file)
    
    # Get all the tables that can exist (either need to be made or already exist). Tables come from the schema JSON
    tables = list(schema.keys())
    
    # If a table does not already exist in the database,
    # then generate a create table statement and execute it on the database
    with engine.connect() as connection:
        for table_name in tables:
            if table_exists(connection, table_name, db_type):
                # print(f"Skipping existing table: {table_name}")
                continue
            
            # It can be that there is an error with creating the table statement (THIS SHOULD NOT HAPPEN!), 
            # if so, then print that there is an error and print the statement for debugging.
            try:
                create_table_statement = generate_create_table_statement(table_name, schema_json_file, db_type)
                connection.execute(text(create_table_statement))
                print(f"[{db_type.to_string_capitalized()}] Created table: {table_name}")
            except Exception as e:
                print(f"Error creating table {table_name}: {e}")
                print(generate_create_table_statement(table_name, schema_json_file, db_type))


def main(engine: Engine, db_type: DBType):

    """
    Adds the reddit data to a database (sqlite or postgresql) without selecting specific lines, it just adds all the data.

    :param engine: Database engine
    :param db_type: The type of the database, either sqlite, mysql, or postgresql
    """
    # Global variables
    global maximum_rows_database

    # Set up the logger
    os.makedirs("logs/summaries", exist_ok=True)
    time_now = time.time()
    log_basename = f'sql_{time_now}.txt'
    log_filename = f"logs/{log_basename}"
    logger = Logger(log_filename)
    sys.stdout = logger

    # Set the path for the db info file according to the db type
    match db_type:
        case DBTypes.SQLITE:
            db_info_file = 'databases/db_info_sqlite_ALL.json'
        case DBTypes.POSTGRESQL:
            db_info_file = 'databases/db_info_postgresql_ALL.json'
        case DBTypes.MYSQL:
            db_info_file = 'databases/db_info_mysql_ALL.json'
        case _:
            raise ValueError(f'[{db_type.to_string_capitalized()}] Unknown database type: {db_type}')

    print(f'[{db_type.to_string_capitalized()}] Only adding new data. To rebuild existing tables, remove them from the {db_info_file} file')

    # If there is no db info file yet, then write an empty JSON such that the file can be accessed
    if not os.path.isfile(db_info_file):
        write_json([], db_info_file)

    clean_json_duplicates(db_info_file)

    tables_exist_skip = set()
    
    # Check if tables in the database are also in the db info file, if not ask user to delete it
    for table in get_tables_database(engine, db_type):
        delete_table = True
        data = load_json(db_info_file)
        for obj in data:
            if table in obj['success_tables']:
                delete_table = False
                break
        if delete_table:
            result_delete = delete_table_db(table, engine, db_type)
            if result_delete:
                tables_exist_skip.add(table)
                print(f'[{db_type.to_string_capitalized()}] Skipping table {table}')

    table_columns = dict()

    # Load config
    data = load_json('config.json')
    data_files = list(data['data_files_tables'].keys())
    data_files_tables = data['data_files_tables']
    maximum_rows_database = data['maximum_rows_database']
    chunk_size = data[db_type.to_string()]['chunk_size']

    create_tables_from_sql(engine, db_type)
   
    # Preparing data
    for file in data_files:
        tables = data_files_tables[file]['sql']
        for table in tables:
            table_columns[table] = get_table_columns(json_schema_path='schemas/db_schema.json', table_name=table)
    
    # Load ignored author names (these can be discarded)
    ignored_author_names = set()
    with open('ignored.txt', 'r', encoding='utf-8') as ignored:
        for ignored_name in ignored:
            ignored_author_names.add(ignored_name.strip().lower())  # Make author names not case-sensitive since it is about the name and not the capitalizing of it

    # Add the data to the SQL database
    for file in data_files:
        tables = data_files_tables[file]['sql']

        tables_to_process = is_file_tables_added_db(file, tables, db_info_file)
        tables_to_process = list(set(tables_to_process) - tables_exist_skip)
        if tables_to_process:
            process_table(data_file=file, tables=tables_to_process, engine=engine, table_columns=table_columns,
                          ignored_author_names=ignored_author_names, chunk_size=chunk_size, db_type=db_type)
            add_file_table_db_info(file, tables_to_process, db_info_file)
            
            # Set index for better read performance
            for table in tables_to_process:
                set_index(engine=engine, table_name=table, db_type=db_type)

    # Rename log file for clarity
    logger.close()
    sys.stdout = sys.__stdout__
    if os.path.isfile(log_filename):  # Only rename it when the file exists.
                                      # If nothing is done, then the file does not exist, for example
        os.rename(log_filename, f'logs/FINISHED_{log_basename}')
