import os
import pandas as pd
from data_to_db.data_to_sql import load_json
from classes.DBType import DBType, DBTypes
import matplotlib.pyplot as plt
import numpy as np
from collections import defaultdict
from statistics import mean

def check_outputs(db_types: list[DBType], query_metrics_file_base_name: str):
    """
    Checks if the length of the output for the same query is the same for every db type.

    :param db_types: The db types to check
    :param query_metrics_file_base_name: The base name of the metrics file
    """
    paths = []
    # Check for each db type whether the output lengths are all the same
    for db_type in db_types:
        path = f'{query_metrics_file_base_name}_{db_type.get_type().display_name.lower()}_{db_type.name_suffix}.json'
        paths.append(path)
        data = load_json(path)
        for k in data.keys():
            if not len(set(data[k]['output_lengths'])) == 1:
                print(f'The length of output for {db_type} must be the same for {k}: {data[k]["output_lengths"]}')

    # Check if the output values for all the db types for the same query are the same length
    data = [load_json(path) for path in paths]
    for i in range(len(data)):
        if i == len(data) - 1:
            next_i = 0
        else:
            next_i = i + 1
        for k in data[i].keys():
            if not set(data[i][k]['output_lengths']) == set(data[next_i][k]['output_lengths']):
                print(f"The values of output for {k} for {db_types[i].display_name} and {db_types[next_i].display_name} are not equal:\n{db_types[i].display_name}: {set(data[i][k]['output_lengths'])}\n{db_types[next_i].display_name}: {set(data[next_i][k]['output_lengths'])}")

def plot_metrics(db_type_sqlite: DBType, db_type_mysql: DBType, db_type_postgresql: DBType, db_type_mongodb: DBType,
               attribute: str, title: str, y_label: str, save_name=None, split_categories=False):
    # Define paths to the metrics files for each DB type
    db_sqlite_path = f'{query_metrics_file_base_name}_{db_type_sqlite.get_type().display_name.lower()}_{db_type_sqlite.name_suffix}.json'
    db_postgresql_path = f'{query_metrics_file_base_name}_{db_type_postgresql.get_type().display_name.lower()}_{db_type_postgresql.name_suffix}.json'
    db_mysql_path = f'{query_metrics_file_base_name}_{db_type_mysql.get_type().display_name.lower()}_{db_type_mysql.name_suffix}.json'
    db_mongodb_path = f'{query_metrics_file_base_name}_{db_type_mongodb.get_type().display_name.lower()}_{db_type_mongodb.name_suffix}.json'

    db_sqlite = load_json(db_sqlite_path)
    db_postgresql = load_json(db_postgresql_path)
    db_mysql = load_json(db_mysql_path)
    db_mongodb = load_json(db_mongodb_path)

    def get_avg_for_queries(queries, db, is_mongodb=False):
        divide_number = 1
        if attribute == 'memories':  # Check if we are dealing with memory, if so make is MB instead of KB (better for plots)
            divide_number = 1024

        # Try to find the queries, if we are dealing with MongoDB the query execution could have been stopped
        # for the reason that it took too long to execute. In that case query results will not be found,
        # thus we discard this queries (the graphs will display that these queries took too long)
        try:
            means = [np.mean(db[q][attribute]) / divide_number for q in queries]
            return means
        except:
            if is_mongodb:
                means = []
                return means

    FONT_SIZE = 18

    if split_categories:
        # Group query names by category (prefix before first '_')
        categories = defaultdict(list)
        for query in db_sqlite.keys():
            cat = query.split('_', 1)[0]
            categories[cat].append(query)

        for cat, cat_queries in categories.items():
            avg_sqlite = get_avg_for_queries(cat_queries, db_sqlite)
            avg_postgresql = get_avg_for_queries(cat_queries, db_postgresql)
            avg_mysql = get_avg_for_queries(cat_queries, db_mysql)
            # Check if the category is simple, if so then include MongoDB because MongoDB only has simple queries
            avg_mongodb = get_avg_for_queries(cat_queries, db_mongodb, is_mongodb=True)

            # By assumption: if average of MongoDB is 0 then that means that MongoDB took too long to query: we will display a high bar
            mongodb_bad_performance = False # Set to True if the bar does not represent the performance, but 'too high to display'
            if len(avg_mongodb) != len(cat_queries) or mean(avg_mongodb) <= 0:
                avg_mongodb = []
                for i in range(len(cat_queries)):
                    avg_mongodb.append(max(avg_sqlite[i], avg_postgresql[i], avg_mysql[i]))
                # max_avg = max(avg_sqlite + avg_postgresql + avg_mysql)  # Take max of the averages of the other databases to get an equally high bar
                # avg_mongodb = [max_avg] * len(cat_queries)
                mongodb_bad_performance = True

            group_spacing = 1.1  # increase to make wider gaps between query groups
            x = np.arange(len(cat_queries)) * group_spacing
            width = 0.25

            fig, ax = plt.subplots(figsize=(16, 14))
            bars1 = ax.bar(x - width, avg_sqlite, width, label='SQLite', color='white', edgecolor='black', hatch='///')
            bars2 = ax.bar(x, avg_postgresql, width, label='PostgreSQL', color='white', edgecolor='black', hatch='\\\\\\')
            bars3 = ax.bar(x + width, avg_mysql, width, label='MySQL', color='white', edgecolor='black', hatch='xxx')

            # Check if the category is simple, if so then include MongoDB because MongoDB only has simple queries
            bars4 = ax.bar(x + width + width, avg_mongodb, width, label='MongoDB', color='white', edgecolor='black', hatch='++')

            ax.set_ylabel(y_label, fontsize=FONT_SIZE)
            ax.set_title(f'{title} – {cat.capitalize()} Queries', fontsize=FONT_SIZE, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels(cat_queries, rotation=45, ha="right", fontsize=FONT_SIZE)
            ax.grid(True, axis='y', linestyle='--', alpha=0.5)
            ax.tick_params(axis='x', labelsize=FONT_SIZE)
            ax.tick_params(axis='y', labelsize=FONT_SIZE)
            ax.legend(fontsize=FONT_SIZE)
            ax.yaxis.get_offset_text().set_fontsize(FONT_SIZE)


            bars_list = [bars1, bars2, bars3, bars4]

            for bars in bars_list:
                for bar in bars:
                    height = bar.get_height()
                    if bars == bars4 and mongodb_bad_performance:  # We annotate the MongoDB bar with a text that will display that it took much longer than the other database
                        ax.annotate(f'>{height:.1f}',
                                    xy=(bar.get_x() + bar.get_width() / 2, height),
                                    xytext=(0, 3),
                                    textcoords="offset points",
                                    ha='center', va='bottom', fontsize=FONT_SIZE, fontstyle='italic')
                    else:
                        ax.annotate(f'{height:.1f}',
                                    xy=(bar.get_x() + bar.get_width() / 2, height),
                                    xytext=(0, 3),
                                    textcoords="offset points",
                                    ha='center', va='bottom', fontsize=FONT_SIZE)
                    ax.legend(fontsize=FONT_SIZE)

            plt.tight_layout()
            plt.show()

            if save_name:
                os.makedirs('plots', exist_ok=True)
                fname = f'plots/{save_name}_{cat}.pdf'
                fig.savefig(fname)
                print(f'Plot saved to {fname}!')
    else:
        queries = list(db_sqlite.keys())
        avg_sqlite = get_avg_for_queries(queries, db_sqlite)
        avg_postgresql = get_avg_for_queries(queries, db_postgresql)
        avg_mysql = get_avg_for_queries(queries, db_mysql)

        x = np.arange(len(queries))
        width = 0.25

        fig, ax = plt.subplots(figsize=(16, 14))
        bars1 = ax.bar(x - width, avg_sqlite, width, label='SQLite', color='white', edgecolor='black', hatch='///')
        bars2 = ax.bar(x, avg_postgresql, width, label='PostgreSQL', color='white', edgecolor='black', hatch='\\\\\\')
        bars3 = ax.bar(x + width, avg_mysql, width, label='MySQL', color='white', edgecolor='black', hatch='xxx')

        ax.set_ylabel(y_label, fontsize=FONT_SIZE)
        ax.set_title(title, fontsize=FONT_SIZE, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(queries, rotation=45, ha="right", fontsize=FONT_SIZE)
        ax.grid(True, axis='y', linestyle='--', alpha=0.5)
        ax.legend(fontsize=FONT_SIZE)
        ax.yaxis.get_offset_text().set_fontsize(FONT_SIZE)

        for bars in [bars1, bars2, bars3]:
            for bar in bars:
                height = bar.get_height()
                ax.annotate(f'{height:.1f}',
                            xy=(bar.get_x() + bar.get_width() / 2, height),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha='center', va='bottom', fontsize=14)
                ax.legend(fontsize=FONT_SIZE)

        plt.tight_layout()
        plt.show()

        if save_name:
            os.makedirs('plots', exist_ok=True)
            fig.savefig(f'plots/{save_name}.pdf')
            print(f'Plot saved to plots/{save_name}.pdf!')

if __name__ == '__main__':
    name_suffix = '1m'
    db_type_sqlite = DBType(DBTypes.SQLITE, name_suffix=name_suffix)
    db_type_mysql = DBType(DBTypes.MYSQL, name_suffix=name_suffix)
    db_type_postgresql = DBType(DBTypes.POSTGRESQL, name_suffix=name_suffix)
    db_type_mongodb = DBType(DBTypes.MONGODB, name_suffix=name_suffix)


    # Dont include mongodb for the checks because it does not have all queries
    db_types = [db_type_sqlite,
                db_type_mysql,
                db_type_postgresql]

    query_metrics_file_base_name = 'output/query_metrics'

    check_outputs(db_types, query_metrics_file_base_name)

    # Plot query execution times
    plot_metrics(db_type_sqlite=db_type_sqlite, db_type_mysql=db_type_mysql, db_type_postgresql=db_type_postgresql,
                 db_type_mongodb=db_type_mongodb, attribute='times',
                 title=f'Average Execution Time per Query by Database (Lower is better) ({name_suffix})',
                 y_label='Average Execution Time (s)', save_name=f'avg_execution_time_per_query_by_db_{name_suffix}',
                 split_categories=True)

    # Plot query memory usage
    plot_metrics(db_type_sqlite=db_type_sqlite, db_type_mysql=db_type_mysql, db_type_postgresql=db_type_postgresql,
                 db_type_mongodb=db_type_mongodb, attribute='memories',
                 title=f'Average Memory Usage per Query by Database (Lower is better) ({name_suffix})',
                 y_label='Average Memory Usage (MB)', save_name=f'avg_memory_usage_per_query_by_db_{name_suffix}',
                 split_categories=True)

