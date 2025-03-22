from sqlalchemy.dialects.postgresql.pg_catalog import pg_get_serial_sequence
import threading
import sqlite3
import os
import re
from collections import deque

def extract_line(line_nr, content_file_path):
    line_nr = int(line_nr)
    count_line_find = 0
    with open(content_file_path, 'r', encoding='utf-8') as f:
        for line in f:
            count_line_find += 1
            if count_line_find == line_nr:
                return line


def get_primary_key(table_name, sql_file_path="db_structure.sql"):
    """
    Gets primary key columns of a table.

    :param table_name: Name of the table.
    :param sql_file_path: Path to the SQL file.

    :return: List of primary key columns.
    """
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()

    with open(sql_file_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    # Replace backticks with double quotes for compatibility
    schema_sql = schema_sql.replace('`', '"')

    # Extract and execute CREATE TABLE statements
    create_table_statements = re.findall(r'CREATE TABLE.*?\);', schema_sql, re.S)
    for statement in create_table_statements:
        try:
            cursor.execute(statement)
        except sqlite3.OperationalError as e:
            print(f"Error executing: {statement[:50]}... -> {e}")

    # Get the primary key columns using PRAGMA index_list and index_info
    cursor.execute(f"PRAGMA index_list({table_name})")
    indexes = cursor.fetchall()

    primary_keys = []

    for index in indexes:
        index_name, is_unique, is_pk = index[1], index[2], index[3]  # Extract index name
        if is_pk:  # Check if it's a PRIMARY KEY index
            cursor.execute(f"PRAGMA index_info({index_name})")
            pk_columns = [row[2] for row in cursor.fetchall()]
            primary_keys.extend(pk_columns)

    conn.close()

    return primary_keys


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

def read_file_reverse(file_path):
    with open(file_path, 'r', encoding='utf-8') as f:
        # Initialize a deque with a fixed size, so it can store lines in reverse order
        lines = deque(maxlen=1)  # Using deque to simulate the reverse reading
        f.seek(0, 2)  # Seek to the end of the file
        buffer = ''

        while f.tell() > 0:
            f.seek(f.tell() - 1024, 0)  # Move the pointer back 1024 bytes
            buffer = f.read(1024) + buffer  # Read the chunk and add to buffer
            lines = deque(buffer.splitlines(), maxlen=1)
            if lines:
                for line in reversed(lines):
                    yield line  # Yield lines in reverse order
