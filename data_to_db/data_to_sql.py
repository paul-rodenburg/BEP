from datetime import datetime
import pandas as pd
import orjson as json
import os
import math
from itertools import chain
from sqlalchemy import text
from tqdm import tqdm
from general import get_primary_key, get_tables_database, get_database_type
from line_counts import get_line_count_file
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import re

progress_bar = None
clean_errors = 0
maximum_rows_database = 0

def unnest(lst):
    """
    Unnests a list, however if a list is not nested it will not unnest it to a list of single characters.

    :param lst: list to unnest

    :return: unnested list
    """
    if all(not isinstance(i, list) for i in lst):
        return lst  # Return as-is if it's already flat
    return list(chain.from_iterable(lst))


def get_table_columns(json_schema_path, table_name) -> list:
    """
    Gets the table columns from a sqlite database file.

    :param json_schema_path: Path to the json schema file.
    :param table_name: Name of the table to extract columns from.

    :return: list of table names in the sqlite database file.
    """
    schema = load_json(json_schema_path)
    columns = list(schema[table_name]['columns'].keys())
    return columns


def process_line_rules(line: dict) -> list[dict]:
    """
    Helper method to process a line for the subreddit_rules table. Unpacks the rule dictionary.

    :param line: Line to process.

    :return: list of dictionaries, each containing one rule
    """
    lines_rules_cleaned = []
    subreddit = line["subreddit"]
    for rule in line["rules"]:
        rule_id = f'{subreddit}_{rule["priority"]}'
        rule = {"rule_id": rule_id, **rule}  # Ensure rule_id the first item (just for better visibility when viewing the database)
        rule['subreddit'] = subreddit
        lines_rules_cleaned.append(rule)

    return lines_rules_cleaned


def clean_line(line_input, tables, table_columns, ignored_author_names) -> dict[str, list[dict]]:
    """
    Gets a line and cleans it for all the tables.

    :param line_input: line to clean
    :param tables: tables for the line
    :param table_columns: columns for the tables
    :param ignored_author_names: author names to ignore
    :return: A dict with as key the table name and value the cleaned line for that table
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
        if table == 'subreddit_rules':
            line = process_line_rules(line)
        if table in 'banned':
            if line['banned_at_utc'] is None and line['banned_by'] is None:
                continue
        if table == 'removed':
            if line['removal_reason'] is None and line['removed_by'] is None:
                continue
        if table == 'wiki':
            try:
                dt = datetime.fromisoformat(line['revision_date'].replace("Z", "+00:00"))
            except Exception as e:
                print(f'KEY ERROR! REVISION DATE: {e}')
                print(line)
                exit(1)
            epoch_time = int(dt.timestamp())
            line['revision_date'] = epoch_time
            pattern = r"/r/([^/]+)"
            subreddit_match = re.search(pattern, line['path'])
            if not subreddit_match:  # If the subreddit could not be found then return None since this data will be useless
                continue
            line['subreddit'] = subreddit_match.group(1)
            line['content'] = line['content'][:500]

        if table == 'post' or table == 'comment':  # To not get any postgreSQL errors
            if int(line['edited']) == 0:
                line['edited'] = False
            else:
                line['edited'] = True

        if table == 'post':
            line['selftext'] = line['selftext'][:500]
        if table == 'author':
            if line['author'].strip().lower() in ignored_author_names:
                continue

        if not isinstance(line, list):
            line = [line]

        cleaned_lines = []
        for l in line:
            l = {key: l[key] for key in items_to_keep if key in l}
            cleaned_lines.append(l)

        cleaned_data[table] = cleaned_lines
    return cleaned_data

seen_keys = {}

def process_cleaned_lines(cleaned_lines_dct, check_duplicates=False) -> dict[str, pd.DataFrame]:
    global seen_keys

    """
    Converts cleaned line entries to DataFrames, optionally ensuring globally unique rows based on primary keys.

    :param cleaned_lines_dct: cleaned lines dictionary
    :param check_duplicates: If True, ensure global uniqueness using seen_keys. If False, skip deduplication.
    :return: A dict with the table name as key and a DataFrame (possibly deduplicated) as value.
    """
    for table_name, data in cleaned_lines_dct.items():
        if not data:
            cleaned_lines_dct[table_name] = None
            continue

        primary_key_column = get_primary_key(table_name)
        if not primary_key_column:
            cleaned_lines_dct[table_name] = None
            continue

        if check_duplicates:
            if table_name not in seen_keys:
                seen_keys[table_name] = set()

        unique_rows = []
        for row in data:
            key = tuple(row.get(pm) for pm in primary_key_column)
            if None in key:
                continue  # skip incomplete primary keys
            if check_duplicates:
                if key in seen_keys[table_name]:
                    continue
                seen_keys[table_name].add(key)
            unique_rows.append(row)

        if not unique_rows:
            cleaned_lines_dct[table_name] = None
            continue

        df = pd.DataFrame(unique_rows)

        # clean up weird data types and null characters
        df = df.map(lambda x: str(x) if isinstance(x, (list, dict)) else x)
        df = df.map(lambda x: x.replace("\x00", "") if isinstance(x, str) else x)

        cleaned_lines_dct[table_name] = df

    return cleaned_lines_dct



sql_count = 0

def process_table(data_file, tables, conn, table_columns, ignored_author_names, chunk_size=10_000):
    """
    Processes tables, so writing the data to a database.

    :param data_file: path to data file
    :param tables: tables to process
    :param conn: database connection
    :param table_columns: dictionary containing tables names as keys and the value are the column names corresponding to the tables
    :param ignored_author_names: author names to ignore.
    :param chunk_size: number of lines to read at a time
    """
    global sql_count

    added_count = 0
    for chunk_data in extract_lines(data_file, tables, table_columns, ignored_author_names, chunk_size):
        for table_name, data in chunk_data.items():
            if data is not None and not data.empty:
                write_to_db(data, table_name, conn, len(chunk_data), chunk_size=chunk_size)
                added_count += 1
    sql_count = 0  # Reset count for the progress bar
    if added_count == 0:
        print(f'Error! All chunks of {tables} were empty')


def extract_lines(data_file, tables, table_columns, ignored_author_names, chunk_size=10_000) -> dict[str, pd.DataFrame]:
    """
    Processes lines in the from the Reddit data file.

    :param data_file: path to the Reddit data file
    :param tables: tables to process
    :param table_columns: dictionary containing tables names as keys and the value are the column names corresponding to the tables
    :param ignored_author_names: author names to ignore
    :param chunk_size: number of lines to read at a time

    :return: A dict with as key the table name and value the cleaned lines for that table in pandas DataFrame
    """
    global progress_bar
    lines_clean = {}
    for table_name in tables:
        lines_clean[table_name] = []


    progress_bar_total = min(get_line_count_file(data_file), maximum_rows_database)
    progress_bar = tqdm(total=progress_bar_total, desc=f"Processing {len(tables)} table(s): {tables} (from {data_file.split('/')[-1]})")

    lines_cleaned_count = 0
    with open(data_file, 'r', encoding='utf-8') as f_data:

        for line in f_data:
            cleaned_data = clean_line(line, tables, table_columns, ignored_author_names)
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

    progress_bar.close()
    if lines_clean:
        yield process_cleaned_lines(lines_clean)


def write_to_db(df, table, conn, len_tables, chunk_size=10_000):
    """
    Write dataframe to database.

    :param df: pandas DataFrame
    :param table: table name
    :param conn: database connection
    :param len_tables: number of tables that are currently being processed (only used to display in tqdm progress bar)
    :param chunk_size: number of lines to read at a time
    """
    global sql_count, progress_bar
    df.to_sql(table, conn, if_exists="append", index=False, chunksize=5000)
    sql_count += 1
    progress_bar.set_postfix_str(f'[{sql_count:,}/{math.ceil(progress_bar.total / chunk_size * len_tables):,} SQL writes]')


def is_file_tables_added_db(data_file, tables, db_info_file) -> list:
    """
    Gets the table names of the table names that are not (fully) processed, so not (competely) added to the database.

    :param data_file: path to the reddit data file
    :param tables: list of table names to check
    :param db_info_file: path to the database info file

    :return: list of table names that need to be added to the database
    """
    if os.path.isfile(db_info_file):
        data = load_json(db_info_file)


        for obj in data:
            if obj['file'] == data_file:
                # Find tables that are in `tables` but not in `success_tables`
                missing_tables = list(set(tables) - set(obj['success_tables']))
                return missing_tables

    # If file doesn't exist or file not found, return all tables
    return tables


def add_file_table_db_info(data_file, tables, db_info_file):
    """
    Adds content to the success_tables in the database specific json file.

    :param data_file: path to the reddit data file
    :param tables: table names to add to the success_tables field in the db_info_file
    :param db_info_file: path to the json db info file
    """
    if not os.path.isfile(db_info_file):
        with open(db_info_file, 'w', encoding='utf-8') as f:
            f.write(json.dumps([], option=json.OPT_INDENT_2))

    data = load_json(db_info_file)

    file_entry = next((obj for obj in data if obj['file'] == data_file), None)

    if file_entry:
        if not all(item in file_entry['success_tables'] for item in tables):
            file_entry['success_tables'].append(tables)
            file_entry['success_tables'] = unnest(file_entry['success_tables'])
            file_entry['success_tables'] = list(set(file_entry['success_tables']))
    else:
        data.append({'file': data_file, 'success_tables': tables})

    with open(db_info_file, 'w', encoding='utf-8') as f:
        f.write(json.dumps(data, option=json.OPT_INDENT_2))

def get_data_file(data_files_tables, table_name) -> str|None:
    """
    Gets the data file path corresponding to the given table name.

    :param data_files_tables: dictionary that maps data file paths to table names
    :param table_name: the name of the table corresponding to a data file.

    :return: path to the data file if table name found, otherwise None
    """
    for file, tables in data_files_tables.items():
        if table_name in tables:
            return file
    return None  # Returns None if the table_name is not found

def load_json(file_path) -> dict|list:
    """
    Loads the content of a json file.

    :param file_path: path to the json file

    :return: a dict with the content of the json file
    """
    with open(file_path, "r") as f:
        content = f.read()
        if not content.strip():
            return []
        return json.loads(content)


def get_tables_to_skip(json_data) -> set:
    """
    Gets the table names of tables that already do not include any duplicates

    :param json_data: data of the database specific info
    """
    tables_to_skip = set()
    for entry in json_data:
        if "duplicates_removed" in entry:
            tables_to_skip.update(entry["duplicates_removed"])
    return tables_to_skip

def update_json_with_table_duplicates(json_file, table_name, data_file):
    """
    Adds the table names of tables where duplicates are removed to the database specific info json file.

    :param json_file: path to json file with database specific info
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
    with open(json_file, "w") as f:
        f.write(json.dumps(json_data, option=json.OPT_INDENT_2))

def clean_json_duplicates(json_file):
    """
    Cleans the json duplicates field, ensures that a table name is only in the duplicates_removed field if in success_tables.

    :param json_file: the path to the json with database specific info
    """
    json_data = load_json(json_file)
    for entry in json_data:
        if "duplicates_removed" in entry and "success_tables" in entry:
            entry["duplicates_removed"] = [table for table in entry["duplicates_removed"] if table in entry["success_tables"]]
    with open(json_file, "wb") as f:
        f.write(json.dumps(json_data, option=json.OPT_INDENT_2))

delete_all = False
def delete_table_db(table_name, engine):
    global delete_all

    """
    If a table exists, asks the user to delete it. If the table does not exist, noting is done.

    :param table_name: name of the table to delete
    :param engine: connection to the database

    :raises ValueError: if the connection type is not supported
    """
    db_type = get_database_type(engine)

    with engine.connect() as conn:
        if table_exists(conn, table_name, db_type):
            if delete_all:
                delete_confirm = 'y'
            else:
                delete_confirm = input(
                    f'Table {table_name} already exists. Delete anyway? (y/n) (or yy to delete all)')
            if delete_confirm.lower().strip() != 'y':
                if delete_confirm.lower().strip() == 'yy':
                    delete_all = True
                    print(f'Deleting all..')
                else:
                    return True
        else:  # Table does not exist
            return


    match db_type:
        case 'sqlite':
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f'Deleted table {table_name} in SQLITE database')
        case 'mysql':
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f'Deleted table {table_name} in MySQL database')
        case 'postgresql':
            with engine.connect() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name} CASCADE"))
            print(f'Deleted table {table_name} in PostgreSQL database')
        case _:
            raise ValueError(f'Unknown database type: {db_type}')


def table_exists(engine, table_name, db_type):
    """
    Checks if a table exists in the database.

    :param engine: connection to the database
    :param table_name: name of the table
    :param db_type: database type

    :raises ValueError: if the database type is not supported

    :return: True if the table exists, False otherwise
    """

    if db_type == 'mysql':
        query = text(f"SHOW TABLES LIKE :table")
        result = engine.execute(query, {'table': table_name}).fetchone()
        return result is not None

    elif db_type == 'postgresql':
        query = text(f"""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_name = :table
            )
        """)
        result = engine.execute(query, {'table': table_name}).fetchone()
        return result[0]

    elif db_type == 'sqlite':
        query = text(f"SELECT name FROM sqlite_master WHERE type='table' AND name=:table")
        result = engine.execute(query, {'table': table_name}).fetchone()
        return result is not None

    else:
        raise ValueError(f'Unknown database type: {db_type}')


def set_index(conn, table_name):
    """
    Sets the index of a database table, ensuring efficient lookups.
    If the index already exists, it will be recreated.

    :param conn: connection to the database
    :param table_name: the name of the table you want to set the index for

    :raises ValueError: if the connection type is not supported
    """
    pms = get_primary_key(table_name)
    db_type = get_database_type(conn)

    print(f"Setting index for table '{table_name}' and columns {pms}...")

    for pm in pms:
        if db_type == 'sqlite':
            # SQLite doesn't use 'connect()', just use the provided 'conn'
            with conn.connect() as engine:
                # Drop the index if it already exists
                engine.execute(text(f"DROP INDEX IF EXISTS index_{pm}"))
                # Create the index
                engine.execute(text(f"CREATE INDEX index_{pm} ON {table_name} ({pm})"))

        elif db_type == 'mysql':
            with conn.connect() as engine:
                # Check if the table exists
                if not table_exists(engine, table_name, db_type):
                    print(f"Table '{table_name}' does not exist. Skipping index creation.")
                    return

                # Check if the index exists
                index_check_query = text(f"""
                    SELECT 1 FROM information_schema.statistics
                    WHERE table_name = :table_name AND index_name = :index_name
                """)
                result = engine.execute(index_check_query,
                                        {'table_name': table_name, 'index_name': f'index_{pm}'}).fetchone()

                # Drop the index if it exists
                if result:
                    drop_index_query = text(f"DROP INDEX index_{pm} ON {table_name}")
                    engine.execute(drop_index_query)

                # Check if the column is of type TEXT or BLOB
                column_type_query = text(f"""
                    SELECT COLUMN_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = :table_name AND COLUMN_NAME = :column_name
                """)
                column_type = engine.execute(column_type_query,
                                             {'table_name': table_name, 'column_name': pm}).fetchone()

                # If the column is TEXT or BLOB, we need to specify the length for the index
                if column_type and ('text' in column_type[0].lower() or 'blob' in column_type[0].lower()):
                    create_index_query = text(f"CREATE INDEX index_{pm} ON {table_name} ({pm}(255))")
                else:
                    create_index_query = text(f"CREATE INDEX index_{pm} ON {table_name} ({pm})")

                # Create the index
                engine.execute(create_index_query)
                engine.commit()

        elif db_type == 'postgresql':
            with conn.connect() as engine:
                # Drop the index if it already exists
                engine.execute(text(f"DROP INDEX IF EXISTS index_{pm}"))
                # Create the index
                engine.execute(text(f"CREATE INDEX index_{pm} ON {table_name} ({pm})"))
                engine.commit()

        else:
            raise ValueError(f'Unknown database type: {db_type}')

def generate_create_table_statement(table_name, schema_json_file, db_type) -> str:
    """
    Makes the CREATE TABLE statements from the json schema file.

    :param schema_json_file: json schema file
    :param table_name: name of the table
    :param db_type: type of db (sqlite, postgreSQL, or mysql)

    :return: CREATE TABLE statement
    """
    schema = load_json(schema_json_file)
    if table_name not in schema:
        raise ValueError(f"Table '{table_name}' not found in the schema.")

    table = schema[table_name]
    columns = table["columns"]
    primary_keys = table.get("primary_key", [])

    lines = []
    if db_type == 'sqlite' or db_type == 'mysql':
        quotation_mark_table_statements = '`'
    elif db_type == 'postgresql':
        quotation_mark_table_statements = '"'
    else:
        raise ValueError(f'Unsupported database type: {db_type}')

    for col_name, col_type in columns.items():
        # Don't add PRIMARY KEY here if there are multiple keys
        line = f'  {quotation_mark_table_statements}{col_name}{quotation_mark_table_statements} {col_type}'
        if isinstance(primary_keys, list) and len(primary_keys) == 1 and col_name in primary_keys:
            line += " PRIMARY KEY"
        lines.append(line)

    # Add composite PRIMARY KEY constraint if needed
    if isinstance(primary_keys, list) and len(primary_keys) > 1:
        primary_keys_alt = [f'{quotation_mark_table_statements}{pk}{quotation_mark_table_statements}' for pk in primary_keys]
        pks_text = ", ".join(primary_keys_alt)
        pk_line = f'  PRIMARY KEY ({pks_text})'
        lines.append(pk_line)

    column_definitions = ",\n".join(lines)
    create_stmt = f'CREATE TABLE {quotation_mark_table_statements}{table_name}{quotation_mark_table_statements} (\n{column_definitions}\n);'

    return create_stmt

def create_tables_from_sql(conn, schema_json_file='schemas/db_schema.json'):
    """
    Creates tables in the database from the provided json schema file,
    only if they don't already exist.

    :param conn: SQLAlchemy engine or sqlite3 connection.
    :param schema_json_file: path to the schema json file
    """
    db_type = get_database_type(conn)
    schema = load_json(schema_json_file)

    tables = list(schema.keys())

    with conn.connect() as connection:
        for table_name in tables:
            if table_exists(connection, table_name, db_type):
                # print(f"Skipping existing table: {table_name}")
                continue

            try:
                create_table_statement = generate_create_table_statement(table_name, schema_json_file, db_type)
                connection.execute(text(create_table_statement))
                print(f"Created table: {table_name}")
            except Exception as e:
                print(f"Error creating table {table_name}: {e}")
                print(generate_create_table_statement(table_name, schema_json_file, db_type))


def generate_sql_database(conn):
    global maximum_rows_database

    """
    Adds the reddit data to a database (sqlite or postgresql) without selecting specific lines, it just adds all the data.

    :param conn: connection to the database
    """
    db_type = get_database_type(conn)
    match db_type:
        case 'sqlite':
            db_info_file = 'databases/db_info_sqlite_ALL.json'
        case 'postgresql':
            db_info_file = 'databases/db_info_postgresql_ALL.json'
        case 'mysql':
            db_info_file = 'databases/db_info_mysql_ALL.json'
        case _:
            raise ValueError(f'Unknown database type: {db_type}')

    print(f'Only adding new data. To rebuild existing tables, remove them from the {db_info_file} file')

    if not os.path.isfile(db_info_file):
        with open(db_info_file, 'w', encoding='utf-8') as f:
            f.write(json.dumps([], option=json.OPT_INDENT_2))

    clean_json_duplicates(db_info_file)

    tables_exist_skip = set()

    for table in get_tables_database(conn):
        delete_table = True
        data = load_json(db_info_file)
        for obj in data:
            if table in obj['success_tables']:
                delete_table = False
                break
        if delete_table:
            result_delete = delete_table_db(table, conn)
            if result_delete:
                tables_exist_skip.add(table)
                print(f'Skipping table {table}')

    table_columns = dict()

    # Load config
    data = load_json('config.json')
    data_files = list(data['data_files_tables'].keys())
    data_files_tables = data['data_files_tables']
    maximum_rows_database = data['maximum_rows_database']


    create_tables_from_sql(conn)
    # Preparing data
    for file in data_files:
        tables = data_files_tables[file]['sql']
        for table in tables:
            table_columns[table] = get_table_columns(json_schema_path='schemas/db_schema.json', table_name=table)

    ignored_author_names = set()
    with open('ignored.txt', 'r', encoding='utf-8') as ignored:
        for ignored_name in ignored:
            ignored_author_names.add(ignored_name.strip().lower())

    # Add the data to the SQL database
    for file in data_files:
        tables = data_files_tables[file]['sql']

        tables_to_process = is_file_tables_added_db(file, tables, db_info_file)
        tables_to_process = list(set(tables_to_process) - tables_exist_skip)
        if tables_to_process:
            process_table(file, tables_to_process, conn, table_columns, ignored_author_names)
            add_file_table_db_info(file, tables_to_process, db_info_file)
            for table in tables_to_process:
                set_index(conn, table)

