import matplotlib.pyplot as plt
import pandas as pd
import os
from general import load_json
from classes.DBType import DBTypes, DBType
import numpy as np

def get_building_time_df(db_types: list[DBType]) -> pd.DataFrame:
    """
    Makes a Pandas dataframe containing the building time for each database type for each datafile.

    :param db_types: The database types to include in the dataframe.

    :return: Dataframe consists the building time for each database type for each datafile.
    """
    base_path_build_times = '../logs/summaries/summary'
    build_times = []
    for db_type in db_types:
        path = f'{base_path_build_times}_{db_type.get_type().display_name.lower()}_{db_type.name_suffix}.json'
        data = load_json(path, make_file_if_not_exists=False)
        for file in data.keys():
            build_times.append({'data_file': file.split('/')[-1], 'time': data[file]['time_elapsed_seconds'], 'db_type': db_type.display_name.split('_')[0]})

    df_build_times = pd.DataFrame(build_times)
    return df_build_times

def plot_building_time(df: pd.DataFrame, save_name=None):
    """
    Plots the building time for each database type for each datafile.

    :param df: The dataframe to plot.
    :param save_name: File name to save the plot as. If None, no plot is saved.
    """
    FONT_SIZE = 12
    # Loop through each unique data_file
    for data_file in df['data_file'].unique():
        # Filter the rows for this data_file
        subset = df[df['data_file'] == data_file]

        # Extract db_type and time for plotting
        db_types = subset['db_type']
        times = subset['time']

        # Create plot
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar(db_types, times, color='blue')

        # Title and labels
        ax.set_title(f'Time per DB for:\n{data_file}', fontsize=14, fontweight='bold')
        ax.set_ylabel('Time (s)', fontsize=FONT_SIZE)
        ax.set_xlabel('Database Type', fontsize=FONT_SIZE)
        ax.tick_params(axis='x', labelsize=FONT_SIZE)
        ax.tick_params(axis='y', labelsize=FONT_SIZE)

        # Save plot with a filename-safe version of the data_file
        plt.tight_layout()
        if save_name:
            fig.savefig(f'plots/{save_name}_{data_file}.pdf')
        plt.show()


if __name__ == '__main__':
    db_type_sqlite = DBType(DBTypes.SQLITE, name_suffix='20m')
    db_type_mysql = DBType(DBTypes.MYSQL, name_suffix='20m')
    db_type_postgresql = DBType(DBTypes.POSTGRESQL, name_suffix='20m')
    db_type_mongodb = DBType(DBTypes.MONGODB, name_suffix='20m')

    db_types = [db_type_sqlite,
                db_type_mysql,
                db_type_postgresql,
                db_type_mongodb]

    df = get_building_time_df(db_types)
    print(df)
    plot_building_time(df, save_name='building_time_by_db')
