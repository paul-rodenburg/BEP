import os
from general import make_postgres_engine, make_mysql_engine, make_sqlite_engine, write_json
from data_to_db.data_to_sql import load_json
from classes.DBType import DBTypes, DBType
from tqdm import tqdm
from general_metrics import update_query_metrics, get_total_queries_number
import tracemalloc
import time
import pandas as pd

def execute_sqlite_query(query: str, db_type: DBType) -> tuple[float, float, int]:
    """
    Executes a sqlite query (string) and return the memory, execution time and length of dataframe (result).

    :param query: The query to execute
    :return: Memory (KB) and time (seconds) needed to execute the query
    """
    tracemalloc.start()
    begin_time = time.time()

    engine = make_sqlite_engine(db_type)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    end_time = time.time()
    current_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    len_df = len(df)
    del df
    return peak_memory / 1024, end_time - begin_time, len_df

def execute_postgres_query(query: str) -> tuple[float, float, int]:
    """
    Executes a PostgreSQL query (string) and return the memory, execution time and length of dataframe (result).

    :param query: The query to execute
    :return: Memory (KB) and time (seconds) needed to execute the query
    """
    tracemalloc.start()
    begin_time = time.time()

    engine = make_postgres_engine()
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    end_time = time.time()
    current_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    len_df = len(df)
    del df
    return peak_memory / 1024, end_time - begin_time, len_df

def execute_mysql_query(db_type: DBType, query: str) -> tuple[float, float, int]:
    """
    Executes a MySQL query (string) and return the memory, execution time and length of dataframe (result).

    :param db_type: Database type to execute the query for.
    :param query: The query to execute
    :return: Memory (KB) and time (seconds) needed to execute the query
    """
    tracemalloc.start()
    begin_time = time.time()

    engine = make_mysql_engine(db_type)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    end_time = time.time()
    current_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    len_df = len(df)
    del df
    return peak_memory / 1024, end_time - begin_time, len_df

def execute_query(db_type: DBType, query: str) -> tuple[float, float, int]:
    """
    Executes a query (string) for a database and return the memory, execution time, and length of dataframe (result).

    :param db_type: Database type to execute the query for.
    :param query: The query to execute.
    :return: (memory (KB), execution time (s), length of dataframe (result)))
    """
    match db_type.get_type():
        case DBTypes.SQLITE:
            return execute_sqlite_query(query, db_type=db_type)
        case DBTypes.POSTGRESQL:
            return execute_postgres_query(query)
        case DBTypes.MYSQL:
            return execute_mysql_query(db_type=db_type, query=query)
        case DBTypes.MONGODB:
            raise ValueError("Run 'query_mongodb_metrics.py' for executing MongoDB queries.")
        case _:
            raise ValueError(f'Unknown database type: {db_type}')


time_prev = 0


def test_query(db_type: DBTypes, query: str) -> bool:
    """
    Tests a query by running it with a low limit and returning whether it executes successfully.

    :param db_type: Database type to test the query on.
    :param query: The query to test.
    :return: True if the query runs successfully, False otherwise.
    """
    global time_prev
    # Modify query to add a LIMIT (or equivalent)
    test_query = query
    test_query += " LIMIT 2"

    try:
        # Execute the query in the respective database
        print(f'[{db_type.display_name}] Evaluating query: {test_query}')
        execute_query(db_type, test_query)
        return True  # Query executed successfully
    except Exception as e:
        print(f"[{db_type.display_name}] Error executing query: {e}")
        exit(1) # Query failed


def execute_all_queries_test(queries_sql_json: dict, db_types: list[DBType]) -> bool:
    """
    Test all queries to check for errors by running them with a low LIMIT.

    :param queries_sql_json: SQL queries to test.
    :param db_types: List of database types to execute the queries for.
    :return: True if all queries pass the test, False if any query fails.
    """
    total = get_total_queries_number(queries_sql_json, db_types)
    print(f'Executing {total} FULL queries. Please wait...')

    pbar = tqdm(total=total, desc='Testing queries')
    for db_type in db_types:
        # Set the right query information
        queries_json = queries_sql_json

        # Test all queries
        for query_type in queries_json.keys():
            queries = queries_json[query_type]['queries']
            for query in queries:
                pbar.update(1)
                pbar.set_postfix_str(f'{db_type.display_name}: {query["name"]}')
                if len(query) == 0:
                    continue
                if not test_query(db_type, query['query']):
                    print(f"[{db_type.display_name}] Error: Query {query['name']} failed during testing.")
                    return False  # Stop if any query fails

    return True  # All queries passed


def execute_full_queries(queries_sql_json: dict, db_types: list[DBType], existing_queries: set = None) -> None:
    """
    Execute the full queries and save metrics.

    :param queries_sql_json: SQL queries to execute.
    :param db_types: List of database types to execute the queries for.
    :param existing_queries: Set of existing queries to skip.
    """
    total = get_total_queries_number(queries_sql_json, db_types)

    print(f'Executing {total} FULL queries. Please wait...')
    print(f'Skipping {len(existing_queries)} queries: {list(existing_queries)}')
    time.sleep(0.1)

    pbar = tqdm(total=total, desc='Executing queries')
    for db_type in db_types:
        # Set the right query information
        queries_json = queries_sql_json

        # Run the queries
        for query_type in queries_json.keys():
            queries = queries_json[query_type]['queries']
            for query in queries:
                pbar.update(1)
                if len(query) == 0:
                    continue
                if existing_queries and query['name'] in existing_queries:
                    continue
                pbar.set_postfix_str(f"{db_type.display_name}: {query["name"]}")
                memory, execution_time, output_length = execute_query(db_type, query['query'])
                update_query_metrics(db_type=db_type, query_name=query['name'], memory=memory,
                                     time=execution_time, output_length=output_length,
                                     query_metrics_file_base_name=query_metrics_file_base_name)


if __name__ == '__main__':
    TEST = False

    # Update working directory to parent folder
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)

    os.makedirs('metrics/output', exist_ok=True)
    query_metrics_file_base_name = 'metrics/output/query_metrics'
    queries_sql_json = load_json('metrics/queries_sql_SUBSET.json')

    # db_types = [DBType(db_type=DBTypes.SQLITE, name_suffix='20m'), DBType(db_type=DBTypes.POSTGRESQL, name_suffix='20m'),
    #             DBType(db_type=DBTypes.MYSQL, name_suffix='20m')]

    db_types = [DBType(db_type=DBTypes.SQLITE, name_suffix='20m')]

    # Get existing queries
    existing_queries = set()

    if TEST:
        # First, test all queries
        all_queries_passed = execute_all_queries_test(queries_sql_json, db_types)
        if all_queries_passed:
            print("All queries passed the test. Now executing the full queries...")
            # Execute the full queries if all tests passed
            execute_full_queries(queries_sql_json, db_types)
        else:
            print("Some queries failed the test. Please check the error messages above.")
    else:
        execute_full_queries(queries_sql_json, db_types, existing_queries=existing_queries)

