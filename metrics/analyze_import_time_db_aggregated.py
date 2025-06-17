import matplotlib.pyplot as plt
import pandas as pd
import os
from general import load_json
from classes.DBType import DBTypes, DBType
import numpy as np
from statistics import mean

def get_building_time_category(db_types: list[DBType], max_line_count: int) -> pd.DataFrame:
    """
    Makes a Pandas dataframe containing the building time for each database type for each datafile.

    :param db_types: The database types to include in the dataframe.
    :param max_line_count: The maximum number of lines in a table that was built.

    :return: Dataframe consisting the building time for each database type for each datafile.
    """
    categories = ['large', 'small']
    building_times = {}

    base_path_build_times = '../logs/summaries/summary'
    for db_type in db_types:
        path = f'{base_path_build_times}_{db_type.get_type().display_name.lower()}_{db_type.name_suffix}.json'
        data = load_json(path, make_file_if_not_exists=False)
        for file in data.keys():
            # Determine category
            db_name = db_type.display_name.split('_')[0]
            if data[file]['line_count'] >= max_line_count:
                category = 'large'
            else:
                category = 'small'

            if db_name not in building_times:
                building_times[db_name] = {}
                for category in categories:
                    building_times[db_name][category] = []
            building_times[db_name][category].append(data[file]['time_elapsed_seconds'])

    # Mean values
    for db_name in building_times.keys():
        for cat in categories:
            building_times[db_name][cat] = mean(building_times[db_name][cat])
    df_build_times = pd.DataFrame(building_times)

    return df_build_times

def plot_building_time(df: pd.DataFrame, name_suffix, save_name=None):
    """
    Plots the building time for each database type for each datafile.

    :param df: The dataframe to plot.
    :param save_name: File name to save the plot as. If None, no plot is saved.
    """
    FONT_SIZE = 12
    # Loop through each unique data_file
    for category in ['large', 'small']:
        subset = df[df.index == category]

        # Extract db_type and time for plotting
        db_types = subset.columns.tolist()
        times = subset.values.flatten()

        # Create plot
        fig, ax = plt.subplots(figsize=(6, 6))
        bars = ax.bar(db_types, times, color='white', edgecolor='black', hatch='///')  # Save bars to add labels later

        # Add value labels on top of bars
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.2f}',  # Format to 2 decimal places
                        xy=(bar.get_x() + bar.get_width() / 2, height),
                        xytext=(0, 3),  # 3 points vertical offset
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=FONT_SIZE)

        # Title and labels
        ax.set_title(f'Import Time of Dataset:\n{category} ({name_suffix})', fontsize=14, fontweight='bold')
        ax.set_ylabel('Time (s)', fontsize=FONT_SIZE)
        ax.set_xlabel('Database System', fontsize=FONT_SIZE)
        ax.tick_params(axis='x', labelsize=FONT_SIZE)
        ax.tick_params(axis='y', labelsize=FONT_SIZE)
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)

        # Save plot
        plt.tight_layout()
        if save_name:
            fig.savefig(f'plots/{save_name}_{category}.pdf')
        plt.show()


if __name__ == '__main__':
    name_suffix = '20m'
    db_type_sqlite = DBType(DBTypes.SQLITE, name_suffix=name_suffix)
    db_type_mysql = DBType(DBTypes.MYSQL, name_suffix=name_suffix)
    db_type_postgresql = DBType(DBTypes.POSTGRESQL, name_suffix=name_suffix)
    db_type_mongodb = DBType(DBTypes.MONGODB, name_suffix=name_suffix)

    db_types = [db_type_sqlite,
                db_type_mysql,
                db_type_postgresql,
                db_type_mongodb]

    df = get_building_time_category(db_types, max_line_count=20_000_000)

    plot_building_time(df, name_suffix, save_name=f'ag_import_time_by_db_{name_suffix}')
