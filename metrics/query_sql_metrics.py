import os
from general import make_postgres_engine, make_mysql_engine, make_sqlite_engine, write_json
from data_to_db.data_to_sql import load_json
from classes.DBType import DBTypes, DBType
from tqdm import tqdm
from metrics.general_metrics import update_query_metrics, get_total_queries_number
import tracemalloc
import time
import pandas as pd
import random

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

def execute_postgres_query(query: str, db_type: DBType) -> tuple[float, float, int]:
    """
    Executes a PostgreSQL query (string) and return the memory, execution time and length of dataframe (result).

    :param query: The query to execute
    :param db_type: Database type to execute the query for.
    :return: Memory (KB) and time (seconds) needed to execute the query
    """
    tracemalloc.start()
    begin_time = time.time()

    engine = make_postgres_engine(db_type=db_type)
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
            return execute_postgres_query(query, db_type=db_type)
        case DBTypes.MYSQL:
            return execute_mysql_query(db_type=db_type, query=query)
        case DBTypes.MONGODB:
            raise ValueError("Run 'query_mongodb_metrics.py' for executing MongoDB queries.")
        case _:
            raise ValueError(f'Unknown database type: {db_type}')


def execute_queries(queries_sql_json: dict, db_types: list[DBType], existing_queries: set = None) -> None:
    """
    Execute the full queries and save metrics.

    :param queries_sql_json: SQL queries to execute.
    :param db_types: List of database types to execute the queries for.
    :param existing_queries: Set of existing queries to skip.
    """
    if existing_queries is None:
        existing_queries = set()

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

def print_order(db_types: list[DBType]):
    """
    Simply print the order of the db_types.

    :param db_types: List of db_types to print.
    """
    order = []
    for db_type in db_types:
        order.append(db_type.display_name)
    print(f'Order of db_types: {order}')

if __name__ == '__main__':
    # Update working directory to parent folder
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)

    os.makedirs('metrics/output', exist_ok=True)
    query_metrics_file_base_name = 'metrics/output/query_metrics'
    queries_sql_json = load_json('metrics/queries_sql_SUBSET.json')

    for i in range(10):
        print(f'Loop {i+1} of 10...')
        name_suffix = '1m'
        db_types = [DBType(db_type=DBTypes.SQLITE, name_suffix=name_suffix), DBType(db_type=DBTypes.POSTGRESQL, name_suffix=name_suffix)]
        random.shuffle(db_types)  # Randomize the order of db_types to remove any advantages of the order
        db_types.append(DBType(db_type=DBTypes.MYSQL, name_suffix=name_suffix))
        print_order(db_types)

        execute_queries(queries_sql_json, db_types)

