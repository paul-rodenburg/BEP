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

def plot_aggregated_metrics(
    db_type_sqlite: DBType,
    db_type_mysql: DBType,
    db_type_postgresql: DBType,
    db_type_mongodb: DBType,
    attribute: str,
    title: str,
    y_label: str,
    save_name=None,
):

    FONT_SIZE = 18
    divide_number = 1024 if attribute == 'memories' else 1

    # Load JSON data
    def load_db(db_type):
        path = f'{query_metrics_file_base_name}_{db_type.get_type().display_name.lower()}_{db_type.name_suffix}.json'
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File {path} not found. Make sure you have executed `query_sql_metrics.py` and `query_mongodb_metrics.py` first")
        return load_json(path)

    dbs = {
        'SQLite': load_db(db_type_sqlite),
        'PostgreSQL': load_db(db_type_postgresql),
        'MySQL': load_db(db_type_mysql),
        'MongoDB': load_db(db_type_mongodb),
    }

    # Get query categories from SQLite (or any DB)
    categories = defaultdict(list)
    for query in dbs['SQLite'].keys():
        cat = query.split('_', 1)[0]
        categories[cat].append(query)

    # Compute average per category per database
    category_means = defaultdict(dict)
    for cat, cat_queries in categories.items():
        for db_name, db in dbs.items():
            values = []
            for q in cat_queries:
                try:
                    v = db[q][attribute]
                    values.append(np.mean(v) / divide_number)
                except:
                    if db_name == 'MongoDB':
                        continue  # MongoDB might miss queries
            if db_name == 'MongoDB' and (len(values) != len(cat_queries) or not values):
                # Fill with high bars if MongoDB failed
                other_means = []
                for other_db in ['SQLite', 'PostgreSQL', 'MySQL']:
                    other_values = [
                        np.mean(dbs[other_db][q][attribute]) / divide_number for q in cat_queries
                    ]
                    other_means.append(mean(other_values))
                custom_value_mongodb = int(input(f'MongoDB ({attribute}) has no value for cat {cat}. Type a integer to set as max (other max: {max(other_means):.1f}) :').strip())
                # values = [max(other_means)]
                values = [custom_value_mongodb]
                category_means[cat][f'{db_name}_too_slow'] = True
            category_means[cat][db_name] = mean(values)

    # Plot
    fig, ax = plt.subplots(figsize=(16, 10))
    desired_order = ['simple', 'nested', 'join', 'analytical']
    categories_list = [cat for cat in desired_order if cat in category_means]
    x = np.arange(len(categories_list))
    width = 0.2

    db_names = ['SQLite', 'PostgreSQL', 'MySQL', 'MongoDB']
    hatch_patterns = ['///', '\\\\\\', 'xxx', '++']
    bar_colors = ['white'] * 4
    edge_colors = ['black'] * 4

    for i, db_name in enumerate(db_names):
        heights = [category_means[cat].get(db_name, 0) for cat in categories_list]
        bars = ax.bar(x + (i - 1.5) * width, heights, width,
                      label=db_name,
                      color=bar_colors[i],
                      edgecolor=edge_colors[i],
                      hatch=hatch_patterns[i])
        # Add annotations
        for j, bar in enumerate(bars):
            height = bar.get_height()
            cat = categories_list[j]
            if category_means[cat].get(f'{db_name}_too_slow', False):
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

    ax.set_ylabel(y_label, fontsize=FONT_SIZE)
    ax.set_xlabel('Query Type', fontsize=FONT_SIZE)
    ax.set_title(title, fontsize=FONT_SIZE, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([cat.capitalize() for cat in categories_list], fontsize=FONT_SIZE)
    ax.grid(True, axis='y', linestyle='--', alpha=0.5)
    ax.tick_params(axis='y', labelsize=FONT_SIZE)
    ax.legend(fontsize=FONT_SIZE)
    ax.yaxis.get_offset_text().set_fontsize(FONT_SIZE)

    plt.tight_layout()
    plt.show()

    if save_name:
        os.makedirs('plots', exist_ok=True)
        fname = f'plots/ag_{save_name}_by_category.pdf'
        fig.savefig(fname)
        print(f'Plot saved to {fname}!')



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
    plot_aggregated_metrics(db_type_sqlite=db_type_sqlite, db_type_mysql=db_type_mysql,
                            db_type_postgresql=db_type_postgresql, db_type_mongodb=db_type_mongodb, attribute='times',
                            title=f'Average Execution Time per Query by Database (Lower is better) ({name_suffix} rows)',
                            y_label='Average Execution Time (s)',
                            save_name=f'avg_execution_time_per_query_by_db_{name_suffix}')

    # Plot query memory usage
    plot_aggregated_metrics(db_type_sqlite=db_type_sqlite, db_type_mysql=db_type_mysql,
                            db_type_postgresql=db_type_postgresql, db_type_mongodb=db_type_mongodb,
                            attribute='memories', title=f'Average Memory Usage per Query by Database (Lower is better) ({name_suffix} rows)',
                            y_label='Average Memory Usage (MB)',
                            save_name=f'avg_memory_usage_per_query_by_db_{name_suffix}')

