import os
import time
import tracemalloc
from typing import Tuple
from pymongo import MongoClient
from tqdm import tqdm
from general_metrics import update_query_metrics, get_total_queries_number
from queries_mongodb import get_queries
from collections.abc import Sized
from classes.DBType import DBTypes, DBType



def execute_query(query, db) -> Tuple[int, float, float]:
    """"
    Executes a MongoDB query and returns the number of results, the execution time and (peak) memory usage in KB.

    :param query: The MongoDB query (dict)
    :param db: The MongoDB db
    :return Tuple[len(result), time, memory]
    """
    try:
        tracemalloc.start()
        begin_time = time.time()
        result = query(db)
        end_time = time.time()
        current_memory, peak_memory = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        if isinstance(result, Sized):
            return len(result), end_time - begin_time, peak_memory / 1024
        else:
            return 1, end_time - begin_time, peak_memory / 1024
    except Exception as e:
        print(f'Error with q: {query}\nError: {e}')
        return -1, -1, -1
        # raise ValueError(f"Error executing '{query}': {e}")


# Execute and print results
def execute_queries(query_definitions: dict, db_type: DBType, query_metrics_file_base_name: str):
    db = MongoClient('mongodb://localhost:27017/')[f'reddit_data_{db_type.name_suffix}']

    pbar = tqdm(total=get_total_queries_number(query_definitions, [db_type]), desc='Executing queries')
    for group_name, group in query_definitions.items():
        queries = group["queries"]
        for q in queries:
            pbar.update(1)
            pbar.set_postfix_str(f'{db_type.display_name}: {q["name"]}')
            collection = db[q["collection"]]
            output_length, execution_time, memory = execute_query(q["query"], collection)

            # Update metrics
            update_query_metrics(db_type=db_type, query_name=q['name'], memory=memory,
                                 time=execution_time, output_length=output_length,
                                 query_metrics_file_base_name=query_metrics_file_base_name)


# Main entry point
if __name__ == "__main__":
    # Update working directory to parent folder
    current_directory = os.getcwd()
    parent_directory = os.path.dirname(current_directory)
    os.chdir(parent_directory)

    # Make db_type object
    db_type = DBType(db_type=DBTypes.MONGODB, name_suffix="1m")
    query_metrics_file_base_name = 'metrics/output/query_metrics'

    LOOP_TIMES = 10
    count = 0
    for _ in range(LOOP_TIMES):
        count += 1
        print(f'Loop {count}/{LOOP_TIMES}')
        # Execute queries
        queries = get_queries()
        # Only get analytical queries to test. Comment the following line if you want to test all queries
        queries = {'analytical': queries['analytical']}

        execute_queries(queries, db_type, query_metrics_file_base_name)
