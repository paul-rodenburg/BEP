import os
from general import make_postgres_engine, make_mysql_engine, make_sqlite_engine, make_mongodb_client, write_json
from data_to_db.data_to_sql import load_json
from classes.DBType import DBTypes, DBType
from tqdm import tqdm
import ast
import tracemalloc
import time
import pandas as pd
from typing import Tuple

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

def execute_mysql_query(db_name: str, query: str) -> tuple[float, float, int]:
    """
    Executes a MySQL query (string) and return the memory, execution time and length of dataframe (result).

    :param query: The query to execute
    :return: Memory (KB) and time (seconds) needed to execute the query
    """
    tracemalloc.start()
    begin_time = time.time()

    engine = make_mysql_engine(db_name)
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    end_time = time.time()
    current_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    len_df = len(df)
    del df
    return peak_memory / 1024, end_time - begin_time, len_df

def execute_mongodb_query(query: str, collection_name: str, query_type: str = "find") -> Tuple[float, float, int]:
    """
    Executes a MongoDB query and returns memory usage (KB), execution time (s), and result length.

    :param query: The MongoDB query (dict)
    :param collection_name: The name of the collection to query
    :param query_type: The type of MongoDB query ('find' or 'aggregate')
    :return: (peak memory in KB, execution time in seconds, number of results)
    """
    tracemalloc.start()
    begin_time = time.time()

    client = make_mongodb_client()
    db = client.get_database()
    collection = db[collection_name]

    data = []
    try:
        if query_type == "find":
            cursor = collection.find(query)
        elif query_type == "aggregate":
            cursor = collection.aggregate(query)
        else:
            raise ValueError(f"Unsupported query_type: {query_type}")
        data = list(cursor)
        df = pd.DataFrame(data)
        len_df = len(df)
        del df
    except Exception as e:
        print(f"Error executing MongoDB query: {e}")
        raise e
    finally:
        client.close()
        end_time = time.time()
        print(end_time - begin_time)
        current_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    return peak_memory / 1024, end_time - begin_time, len_df

def execute_query(db_type: DBType, query: str, collection_name=None) -> tuple[float, float, int]:
    """
    Executes a query (string) for a database and return the memory, execution time, and length of dataframe (result).

    :param db_type: Database type to execute the query for.
    :param query: The query to execute.
    :param collection_name: Only for MongoDB. Name of the collection to execute the query for.
    :return: (memory (KB), execution time (s), length of dataframe (result)))
    """
    match db_type.get_type():
        case DBTypes.SQLITE:
            return execute_sqlite_query(query, db_type=db_type)
        case DBTypes.POSTGRESQL:
            return execute_postgres_query(query)
        case DBTypes.MYSQL:
            db_name = load_json('config.json')['mysql']['db_name']
            return execute_mysql_query(db_name=db_name, query=query)
        case DBTypes.MONGODB:
            return execute_mongodb_query(query=query, collection_name='posts')
        case _:
            raise ValueError(f'Unknown database type: {db_type}')


def update_query_metrics(db_type: DBType, query_name: str, time: float, memory: float, output_length: int, query_metrics_file_base_name: str):
    """
    Updates the JSON file containing the query times and memory needed for each query for a database.

    :param db_type: Type of database.
    :param query_name: Name of the query.
    :param time: Time needed to execute the query.
    :param memory: The memory (KB) needed to execute the query.
    :param output_length: Length of the output dataframe.
    :param query_metrics_file_base_name: JSON base name for the query times.
    """
    path = f'{query_metrics_file_base_name}_{db_type.get_type().display_name.lower()}_{db_type.name}.json'
    current_metrics_data = load_json(path)
    if query_name not in current_metrics_data:
        current_metrics_data[query_name] = {}

    # Add time
    if 'time' not in current_metrics_data[query_name]:
        current_metrics_data[query_name]['times'] = []
    current_metrics_data[query_name]['times'].append(int(time))

    # Add memory
    if 'memory' not in current_metrics_data[query_name]:
        current_metrics_data[query_name]['memories'] = []
    current_metrics_data[query_name]['memories'].append(memory)

    # Add output length of dataframe
    if 'output_length' not in current_metrics_data[query_name]:
        current_metrics_data[query_name]['output_lengths'] = []
    current_metrics_data[query_name]['output_lengths'].append(output_length)

    write_json(current_metrics_data, path)

def get_total_queries_number(json_queries: dict, db_types: list[DBType]) -> int:
    """
    Gets the total number of queries for all query types.

    :param json_queries: JSON containing the queries for each query type.
    :param db_types: List of database types to include in the count.
    :return: Total number of queries.
    """
    total_queries = 0
    for query_type in json_queries.keys():
        queries = json_queries[query_type]['queries']
        total_queries += len(queries)

    # Multiply by the number of databases, because each query is executed on each database
    return total_queries * len(db_types)

def load_mongodb_queries(path: str) -> dict:
    """
    Converts all MongoDB query strings in the input JSON to actual dictionaries.

    :param json_data: The original JSON data containing query strings.
    :return: A new dict with the same structure but query strings turned into dicts.
    """
    json_mongo = load_json(path)
    return json_mongo
    for category in json_mongo.values():
        for query in category["queries"]:
            try:
                query["query"] = ast.literal_eval(query["query"])
            except Exception as e:
                print(f"[MongoDB] Failed to parse query '{query['name']}': {e}")
                print(query['query'])
    return json_mongo
time_prev = 0


def test_query(db_type: DBTypes, query: str, collection_name=None) -> bool:
    """
    Tests a query by running it with a low limit and returning whether it executes successfully.

    :param db_type: Database type to test the query on.
    :param query: The query to test.
    :param collection_name: Only for MongoDB. Name of the collection to execute the query for.
    :return: True if the query runs successfully, False otherwise.
    """
    global time_prev
    # Modify query to add a LIMIT (or equivalent)
    test_query = query
    if db_type.is_sql():
        test_query += " LIMIT 2"
    elif db_type == DBTypes.MONGODB:
        test_query = query  # Modify for MongoDB if needed, depending on the query structure

    try:
        # Execute the query in the respective database
        print(f'[{db_type.display_name}] Evaluating query: {test_query}')
        execute_query(db_type, test_query, collection_name)
        return True  # Query executed successfully
    except Exception as e:
        print(f"[{db_type.display_name}] Error executing query: {e}")
        exit(1) # Query failed


def execute_all_queries_test(queries_sql_json: dict, queries_mongodb_json: dict, db_types: list[DBType]) -> bool:
    """
    Test all queries to check for errors by running them with a low LIMIT.

    :param queries_sql_json: SQL queries to test.
    :param queries_mongodb_json: MongoDB queries to test.
    :param db_types: List of database types to execute the queries for.
    :return: True if all queries pass the test, False if any query fails.
    """
    total_sql = get_total_queries_number(queries_sql_json, db_types)
    total_mongo = get_total_queries_number(queries_mongodb_json, db_types)
    total = total_sql + total_mongo
    print(f'Executing {total} FULL queries. Please wait...')

    pbar = tqdm(total=total, desc='Testing queries')
    for db_type in db_types:
        # Set the right query information
        if db_type.is_sql():
            queries_json = queries_sql_json
        else:
            queries_json = queries_mongodb_json

        # Test all queries
        for query_type in queries_json.keys():
            queries = queries_json[query_type]['queries']
            for query in queries:
                pbar.update(1)
                pbar.set_postfix_str(f'{db_type.display_name}: {query["name"]} ({time_prev}s prev)')
                if len(query) == 0:
                    continue
                if not test_query(db_type, query['query']):
                    print(f"[{db_type.display_name}] Error: Query {query['name']} failed during testing.")
                    return False  # Stop if any query fails

    return True  # All queries passed


def execute_full_queries(queries_sql_json: dict, queries_mongodb_json: dict, db_types: list[DBType]) -> None:
    """
    Execute the full queries and save metrics.

    :param queries_sql_json: SQL queries to execute.
    :param queries_mongodb_json: MongoDB queries to execute.
    :param db_types: List of database types to execute the queries for.
    """
    total = get_total_queries_number(queries_sql_json, db_types) + get_total_queries_number(queries_mongodb_json, db_types)

    print(f'Executing {total} FULL queries. Please wait...')
    time.sleep(0.1)

    pbar = tqdm(total=total, desc='Executing queries')
    for db_type in db_types:
        # Set the right query information
        if db_type.get_type().is_sql():
            queries_json = queries_sql_json
        else:
            queries_json = queries_mongodb_json

        # Run the queries
        for query_type in queries_json.keys():
            queries = queries_json[query_type]['queries']
            for query in queries:
                if len(query) == 0:
                    continue
                pbar.update(1)
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
    queries_mongodb_json = load_mongodb_queries('metrics/queries_mongodb_SUBSET.json')

    # db_types = [DBType(db_type=DBTypes.SQLITE, name='ALL'), DBType(db_type=DBTypes.POSTGRESQL, name='ALL'),
    #             DBType(db_type=DBTypes.MYSQL, name='ALL'),DBType(db_type=DBTypes.MONGODB, name='ALL')]

    db_types = [DBType(db_type=DBTypes.SQLITE, name='20m')]

    if TEST:
        # First, test all queries
        all_queries_passed = execute_all_queries_test(queries_sql_json, queries_mongodb_json, db_types)
        if all_queries_passed:
            print("All queries passed the test. Now executing the full queries...")
            # Execute the full queries if all tests passed
            execute_full_queries(queries_sql_json, queries_mongodb_json, db_types)
        else:
            print("Some queries failed the test. Please check the error messages above.")
    else:
        execute_full_queries(queries_sql_json, queries_mongodb_json, db_types)

