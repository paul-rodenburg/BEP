import os
import time
from sqlalchemy import text
from general import make_postgres_engine, make_mysql_engine, make_sqlite_engine, make_mongodb_engine, write_json
from data_to_db.data_to_sql import load_json
from classes.DBType import DBTypes, DBType
from tqdm import tqdm

def execute_sqlite_query(query: str) -> int:
    """
    Executes a sqlite query (string) and return the execution time.

    :param query: The query to execute
    :return: time (seconds) needed to execute the query
    """
    begin_time = time.time()
    engine = make_sqlite_engine()
    with engine.connect() as conn:
        conn.execute(text(query))
    end_time = time.time()
    total = end_time - begin_time
    return int(total)


def execute_postgres_query(query: str) -> int:
    """
    Executes a PostgreSQL query (string) and return the execution time.

    :param query: The query to execute
    :return: Time (seconds) needed to execute the query
    """
    begin_time = time.time()
    engine = make_postgres_engine()
    with engine.connect() as conn:
        conn.execute(text(query))
    end_time = time.time()
    total = end_time - begin_time
    return int(total)

def execute_mysql_query(db_name: str, query: str) -> int:
    """
    Executes a mysql query (string) and return the execution time.

    :param query: The query to execute
    :return: Time (seconds) needed to execute the query
    """
    begin_time = time.time()
    engine = make_mysql_engine(db_name)
    with engine.connect() as conn:
        conn.execute(text(query))
    end_time = time.time()
    total = end_time - begin_time
    return int(total)


def execute_mongdb_query(query: str) -> int:
    """
    Executes a mongdb query (string) and return the execution time.

    :param query: The query to execute
    :return: Time (seconds) needed to execute the query
    """
    begin_time = time.time()
    engine = make_mongodb_engine()
    with engine.connect() as conn:
        conn.execute(text(query))
    end_time = time.time()
    total = end_time - begin_time
    return int(total)

def execute_query(db_type: DBTypes, query: str) -> int:
    """
    Executes a query (string) for a database and return the execution time.

    :param db_type: Database type to execute the query for.
    :param query: The query to execute.
    :return: Time needed to execute the query.
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
            print('MongoDB not supported yet.')
            return -1
        case _:
            raise ValueError(f'Unknown database type: {db_type}')


def update_query_times(db_type: DBTypes, query_name: str, time: int, query_times_base_name: str):
    """
    Updates the JSON file containing the query times for each query for a database.

    :param db_type: Type of database.
    :param query_name: Name of the query.
    :param time: Time needed to execute the query.
    :param query_times_base_name: JSON base name for the query times.
    """
    path = f'{query_times_base_name}_{db_type.display_name.lower()}.json'
    current_times = load_json(path)
    if query_name not in current_times:
        current_times[query_name] = {}
    if 'time' not in current_times[query_name]:
        current_times[query_name]['times'] = []
    current_times[query_name]['times'].append(time)
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

if __name__ == '__main__':
    # Update working directory to parent folder
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)


    os.makedirs('metrics/query_times', exist_ok=True)
    query_times_base_name = 'metrics/query_times/query_times'
    queries_json = load_json('metrics/queries_sql.json')


    pbar = tqdm(total=get_total_queries_number(queries_json), desc='Executing queries')
    for query_type in queries_json.keys():
        queries = queries_json[query_type]['queries']
        for query in queries:
            if len(query) == 0:
                continue
            for db_type in DBTypes:
                pbar.update(1)
                pbar.set_postfix_str(f'{db_type.display_name}: {query['name']}')
                execution_time = execute_query(db_type, query['query'])
                update_query_times(db_type, query['name'], execution_time, query_times_base_name)
