import pandas as pd
from data_to_db.data_to_sql import load_json
from classes.DBType import DBTypes, DBType
from general_metrics import expand_excel

def get_creation_time_table(db_type: str, data_file: str) -> int:
    """
    Gets the time (seconds) it took to create a table from the data in a data file.

    :param db_type: The type of database.
    :param data_file: Path to the datafile.
    :return: The time (seconds) it took to create the table.
    """
    log_path = f'../logs/summaries/summary_{db_type}.json'
    log = load_json(log_path)
    creation_time = log[data_file]['time_elapsed_seconds']
    return creation_time

def get_total_creation_time(db_type: str) -> int:
    """
    Gets the time (seconds) it took to create an entire database.

    :param db_type: The type of database.
    :return: The time (seconds) it took to create the database.
    """
    log_path = f'../logs/summaries/summary_{db_type}.json'
    log = load_json(log_path)
    creation_time = 0
    for data_file in log.keys():
        creation_time += log[data_file]['time_elapsed_seconds']
    if creation_time == 0:  # Then the database is not already generated, so return -1
        return -1
    return creation_time

def get_datafiles(db_type: str) -> list:
    """
    Gets list of tables in a database.

    :param db_type: The type of database.
    :return: A list of tables in the database.
    """
    log_path = f'../logs/summaries/summary_{db_type}.json'
    log = load_json(log_path)
    return list(log.keys())

def create_dataframe_durations() -> pd.DataFrame:
    """
    Creates a DataFrame with the creation times of all databases.

    :return: A DataFrame with the creation times of all databases.
    """
    databases = [db_type.value for db_type in DBTypes]
    metrics_databases = []
    for database in databases:
        metric_data = {'database': database}
        for data_file in get_datafiles(database):
            data_file_truncated = data_file.split('/')[-1]
            metric_data[data_file_truncated] = get_creation_time_table(database, data_file)
        metric_data['total'] = get_total_creation_time(database)
        metrics_databases.append(metric_data)
    df = pd.DataFrame(metrics_databases)
    return df

if __name__ == '__main__':
    df = create_dataframe_durations()
    creation_time_excel_path = 'creation_time.xlsx'
    df.to_excel(creation_time_excel_path, index=False)
    expand_excel(creation_time_excel_path)
