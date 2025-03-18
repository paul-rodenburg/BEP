from sqlalchemy.dialects.postgresql.pg_catalog import pg_get_serial_sequence
from tqdm import tqdm
import threading
import sqlite3
import os
import re

def get_line_count_file(file_path: str, timeout=20):
    """"
    Gets the number of lines in a file.

    :param file_path: Path to the file.
    :param timeout: Timeout in seconds, set to None for no timeout.

    :return: Number of lines in a file (can be less if timeout is reached).

    """
    count = 0
    file_path_short = file_path.split('/')[-1]
    stop_event = threading.Event()
    if timeout is None:
        timeout_text = 'No timeout'
    else:
        timeout_text = f'Timeout: {timeout} seconds'
    def count_lines():
        nonlocal count
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for _ in tqdm(f, desc=f'Counting lines in {file_path_short} ({timeout_text})'):
                    if stop_event.is_set():
                        break
                    count += 1
        except Exception as e:
            print(f"\nError reading file: {e}")

    thread = threading.Thread(target=count_lines)
    thread.start()

    if timeout is not None:
        thread.join(timeout)
        if thread.is_alive():
            stop_event.set()
            print("\nTimeout reached! Returning partial count.")
            thread.join()  # Ensure thread stops before returning
    else:
        thread.join()  # Wait indefinitely if no timeout

    return count


def get_primary_key(table_name, sql_file_path="db_structure.sql"):
    """"
    Gets primary key of a table.

    :param table_name: Name of the table.
    :param sql_file_path: Path to the sql file.

    return: Primary key of the table.

    """
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    with open(sql_file_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    # Replace backticks with double quotes
    schema_sql = schema_sql.replace('`', '"')

    # Extract CREATE TABLE statements only
    create_table_statements = re.findall(r'CREATE TABLE.*?\);', schema_sql, re.S)

    # Execute CREATE TABLE statements first
    for statement in create_table_statements:
        try:
            cursor.execute(statement)
        except sqlite3.OperationalError as e:
            print(f"Error executing: {statement[:50]}... -> {e}")

    # Get primary key column for the table
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = cursor.fetchall()

    # Find the primary key column
    primary_keys = [col[1] for col in columns if col[5] == 1]

    conn.close()
    return primary_keys[0] if len(primary_keys) == 1 else primary_keys


# def mysql_to_sqlite(sqlite_db_file, mysql_sql_file='db_structure.sql'):
#     # Open the MySQL SQL dump file
#     with open(mysql_sql_file, 'r', encoding='utf-8') as f:
#         mysql_sql = f.read()
#
#     # Modify MySQL-specific syntax to SQLite-compatible syntax
#     sqlite_sql = mysql_sql
#
#     # Convert MySQL data types and syntax to SQLite
#     sqlite_sql = sqlite_sql.replace('AUTO_INCREMENT', 'AUTOINCREMENT')  # MySQL-specific
#     sqlite_sql = sqlite_sql.replace('ENGINE=InnoDB', '')  # Remove MySQL engine directives
#     sqlite_sql = sqlite_sql.replace('`', '"')  # Replace backticks with double quotes
#     sqlite_sql = sqlite_sql.replace('BOOL', 'INTEGER')  # MySQL's BOOL is SQLite's INTEGER
#     sqlite_sql = sqlite_sql.replace('TINYINT(1)', 'INTEGER')  # Handle TINYINT(1)
#     sqlite_sql = sqlite_sql.replace('TEXT NOT NULL', 'TEXT')  # MySQL's TEXT NOT NULL to SQLite format
#
#     # SQLite has different ways of handling foreign keys (they are disabled by default)
#     # Ensure that foreign keys are enabled on SQLite
#     sqlite_sql = 'PRAGMA foreign_keys = ON;\n' + sqlite_sql
#
#     # Create the SQLite database connection
#     conn = sqlite3.connect(sqlite_db_file)
#     cursor = conn.cursor()
#
#     # Split the SQL into individual statements using sqlparse
#     statements = sqlparse.split(sqlite_sql)
#
#     # Execute each statement in SQLite
#     for stmt in statements:
#         stmt = stmt.strip()
#         if stmt:  # Execute only non-empty statements
#             try:
#                 cursor.execute(stmt)
#             except:
#                 pass
#
#     # Commit the changes and close the connection
#     conn.commit()
#     conn.close()
#
#
# def get_all_tables_from_db(sqlite_db_file):
#     # Connect to the SQLite database
#     conn = sqlite3.connect(sqlite_db_file)
#     cursor = conn.cursor()
#
#     # Query sqlite_master to get all table names
#     cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#     tables = cursor.fetchall()
#
#     # Close the connection
#     conn.close()
#
#     # Return a list of table names
#     return [table[0] for table in tables]
#
#
# def get_tables_database(sql_file_path="db_structure.sql"):
#     temp_db_file = 'temp.db'
#     mysql_to_sqlite(temp_db_file, sql_file_path)
#     tables = get_all_tables_from_db(temp_db_file)
#     os.remove(temp_db_file)
#     return tables

def get_tables_database(database_path='data.db'):
    # Connect to the SQLite database
    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # Query to get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    # Fetch all results
    tables = cursor.fetchall()

    # Print table names
    table_list = [table[0] for table in tables]

    # Close the connection
    conn.close()

    return table_list
