import os
import time
import tracemalloc
import pandas as pd
import pymongo
from general import make_postgres_engine, make_mysql_engine, make_sqlite_engine, make_mongodb_client, write_json
from data_to_db.data_to_sql import load_json
from classes.DBType import DBTypes, DBType
from tqdm import tqdm
import ast

def execute_sqlite_query(query: str) -> tuple[float, float, int]:
    """
    Executes a sqlite query (string) and return the memory, execution time and length of dataframe (result).

    :param query: The query to execute
    :return: Memory (KB) and time (seconds) needed to execute the query
    """
    tracemalloc.start()
    begin_time = time.time()

    engine = make_sqlite_engine()
    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    end_time = time.time()
    current_memory, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    return peak_memory / 1024, end_time - begin_time, len(df)

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
    return peak_memory / 1024, end_time - begin_time, len(df)

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
    return peak_memory / 1024, end_time - begin_time, len(df)

def execute_mongodb_query(query: dict, collection_name: str, query_type: str = "find") -> Tuple[float, float, int]:
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
    finally:
        client.close()
        end_time = time.time()
        current_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    return peak_memory / 1024, end_time - begin_time, len(df)

def execute_query(db_type: DBTypes, query: str, collection_name=None) -> tuple[float, int, int]:
    """
    Executes a query (string) for a database and return the memory, execution time, and length of dataframe (result).

    :param db_type: Database type to execute the query for.
    :param query: The query to execute.
    :param collection_name: Only for MongoDB. Name of the collection to execute the query for.
    :return: Time and memory (KB) that are needed to execute the query.
    """
    match db_type:
        case DBTypes.SQLITE:
            return execute_sqlite_query(query)
        case DBTypes.POSTGRESQL:
            return execute_postgres_query(query)
        case DBTypes.MYSQL:
            db_name = load_json('config.json')['mysql']['db_name']
            return execute_mysql_query(db_name=db_name, query=query)
        case DBTypes.MONGODB:
            # print('MongoDB not supported yet.')
            return execute_mongodb_query(query=query, collection_name='posts')
        case _:
            raise ValueError(f'Unknown database type: {db_type}')


def update_query_metrics(db_type: DBTypes, query_name: str, time: int, memory: float, output_length: int, query_metrics_file_base_name: str):
    """
    Updates the JSON file containing the query times and memory needed for each query for a database.

    :param db_type: Type of database.
    :param query_name: Name of the query.
    :param time: Time needed to execute the query.
    :param memory: The memory (KB) needed to execute the query.
    :param output_length: Length of the output dataframe.
    :param query_metrics_file_base_name: JSON base name for the query times.
    """
    path = f'{query_metrics_file_base_name}_{db_type.display_name.lower()}.json'
    current_times = load_json(path)
    if query_name not in current_times:
        current_times[query_name] = {}

    # Add time
    if 'time' not in current_times[query_name]:
        current_times[query_name]['times'] = []
    current_times[query_name]['times'].append(time)

    # Add memory
    if 'memory' not in current_times[query_name]:
        current_times[query_name]['memories'] = []
    current_times[query_name]['memories'].append(memory)

    # Add output length of dataframe
    if 'output_length' not in current_times[query_name]:
        current_times[query_name]['output_lengths'] = []
    current_times[query_name]['output_lengths'].append(output_length)

    write_json(current_times, path)

def get_total_queries_number(json_queries: dict) -> int:
    """
    Gets the total number of queries for all query types.

    :param json_queries: JSON containing the queries for each query type.
    :return: Total number of queries.
    """
    total_queries = 0
    for query_type in json_queries.keys():
        queries = json_queries[query_type]['queries']
        total_queries += len(queries)
    return total_queries * 3  # Multiply by 3 because 3 databases: MySQL, sqlite, PostgreSQL

def load_mongodb_queries(path: str) -> dict:
    """
    Converts all MongoDB query strings in the input JSON to actual dictionaries.

    :param json_data: The original JSON data containing query strings.
    :return: A new dict with the same structure but query strings turned into dicts.
    """
    json_mongo = load_json(path)
    for category in json_mongo.values():
        for query in category["queries"]:
            try:
                query["query"] = ast.literal_eval(query["query"])
            except Exception as e:
                print(f"Failed to parse query '{query['name']}': {e}")
    return json_mongo



if __name__ == '__main__':
    # Update working directory to parent folder
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)


    os.makedirs('metrics/output', exist_ok=True)
    query_metrics_file_base_name = 'metrics/output/query_metrics'
    queries_sql_json = load_json('metrics/queries_sql.json')
    queries_mongodb_json = load_mongodb_queries('metrics/queries_mongodb.json')


    pbar = tqdm(total=get_total_queries_number(queries_sql_json), desc='Executing queries')
    for db_type in DBTypes:
        # Set the right query information
        if db_type.is_sql():
            queries_json = queries_sql_json
        else:
            queries_json = queries_mongodb_json

        # Run the queries
        for query_type in queries_json.keys():
            queries = queries_sql_json[query_type]['queries']
            for query in queries:
                if len(query) == 0:
                    continue
                pbar.update(1)
                pbar.set_postfix_str(f'{db_type.display_name}: {query['name']}')
                memory, execution_time, output_length = execute_query(db_type, query['query'])
                update_query_metrics(db_type=db_type, query_name=query['name'], memory=memory,
                                     time=execution_time, output_length=output_length, query_metrics_file_base_name=query_metrics_file_base_name)
