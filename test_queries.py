import time
from sqlalchemy import text
from general import make_postgres_engine, make_mysql_engine, make_sqlite_engine, make_mongodb_engine


def execute_sqlite_query(query: str) -> float:
    """
    Executes a sqlite query (string) and return the execution time.

    :param query: the query to execute

    :return: time (seconds) needed to execute the query
    """
    begin_time = time.time()
    engine = make_sqlite_engine()
    with engine.connect() as conn:
        conn.execute(text(query))
    end_time = time.time()
    total = end_time - begin_time
    return total


def execute_postgres_query(query: str) -> float:
    """
    Executes a postgreSQL query (string) and return the execution time.

    :param query: the query to execute

    :return: time (seconds) needed to execute the query
    """
    begin_time = time.time()
    engine = make_postgres_engine()
    with engine.connect() as conn:
        conn.execute(text(query))
    end_time = time.time()
    total = end_time - begin_time
    return total

def execute_mysql_query(query: str) -> float:
    """
    Executes a mysql query (string) and return the execution time.

    :param query: the query to execute

    :return: time (seconds) needed to execute the query
    """
    begin_time = time.time()
    engine = make_mysql_engine()
    with engine.connect() as conn:
        conn.execute(text(query))
    end_time = time.time()
    total = end_time - begin_time
    return total


def execute_mongdb_query(query: str) -> float:
    """
    Executes a mongdb query (string) and return the execution time.

    :param query: the query to execute

    :return: time (seconds) needed to execute the query
    """
    begin_time = time.time()
    engine = make_mongodb_engine()
    with engine.connect() as conn:
        conn.execute(text(query))
    end_time = time.time()
    total = end_time - begin_time
    return total

    return time


if __name__ == '__main__':
    # Test example
    print(execute_sqlite_query('SELECT COUNT(*) FROM author'))