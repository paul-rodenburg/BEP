import pymysql
from pymongo import MongoClient
import psycopg2
import os
from matplotlib import pyplot as plt
import pandas as pd
from general import load_json
import math
def get_mysql_db_size(host, user, password, db_name):
    connection = pymysql.connect(host=host, user=user, password=password)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT SUM(data_length + index_length)
                FROM information_schema.tables
                WHERE table_schema = %s
            """, (db_name,))
            size_bytes = cursor.fetchone()[0]
            return size_bytes or 0
    finally:
        connection.close()


def get_mongodb_db_size(uri, db_name):
    client = MongoClient(uri)
    db = client[db_name]
    stats = db.command("dbstats")
    return stats.get("storageSize", 0)


def get_postgres_db_size(host, user, password, db_name):
    connection = psycopg2.connect(host=host, user=user, password=password, dbname=db_name)
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT pg_database_size(%s);", (db_name,))
            size_bytes = cursor.fetchone()[0]
            return size_bytes
    finally:
        connection.close()


def format_size(bytes_val):
    bytes_val = float(bytes_val)  # Ensure float division
    for unit in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if bytes_val < 1024.0:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024.0

def convert_bytes_to_gb(bytes_val):
    return float(bytes_val / 1024 / 1024 / 1024)

def get_sqlite_db_size(db_path):
    if not os.path.isfile(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")
    return os.path.getsize(db_path)

def plot_databases_sizes(df, name_suffix):
    FONT_SIZE = 16
    ax = df.plot.bar(x='database', y='size', color='white', edgecolor='black', hatch='///', figsize=(6, 6))
    ax.set_ylabel('Size (GB)', fontsize=FONT_SIZE)
    ax.set_title(f'Disk Usage ({name_suffix})', fontweight='bold', fontsize=FONT_SIZE+2)
    ax.set_xticklabels(df['database'], rotation=45, ha="right")
    ax.tick_params(axis='x', labelsize=FONT_SIZE)
    ax.tick_params(axis='y', labelsize=FONT_SIZE)
    ax.legend().remove()
    ax.set_xlabel('')
    ax.grid(True, axis='y', linestyle='--', alpha=0.5)
    max_size_value = df['size'].max()
    if max_size_value < 10:
        max_y_value = math.ceil(max_size_value)
    else:
        max_y_value = math.ceil(max_size_value / 10) * 10
    ax.set(ylim=(0, max_y_value))

    # Add value labels on top of bars
    for bar in ax.patches:
        height = bar.get_height()
        ax.annotate(f'{height:.2f}',
                    xy=(bar.get_x() + bar.get_width() / 2, height),
                    xytext=(0, 3),  # Offset above the bar
                    textcoords='offset points',
                    ha='center', va='bottom', fontsize=FONT_SIZE)

    plt.tight_layout()

    # Save the plot
    os.makedirs('plots', exist_ok=True)
    plt.savefig(f'plots/disk_usage_{name_suffix}.pdf')

    plt.show()


if __name__ == '__main__':
    name_suffix = '1m'
    config_data = load_json('../config.json')

    # Config data MySQL
    host_mysql = config_data['mysql']['host']
    username_mysql = config_data['mysql']['username']
    password_mysql = config_data['mysql']['password']
    db_name_base_mysql = 'reddit_data'

    # Config data MongoDB
    host_mongodb = config_data['mongodb']['host']
    port_mongodb = config_data['mongodb']['port']
    db_name_base_mongodb = 'reddit_data'

    # Config data PostgreSQL
    host_postgresql = config_data['postgresql']['host']
    username_postgresql = config_data['postgresql']['username']
    password_postgresql = config_data['postgresql']['password']
    db_name_base_postgresql = 'reddit_data'

    # Config data SQLite
    folder_db_sqlite = config_data['sqlite']['db_folder']
    db_name_base_sqlite = 'reddit_data'

    # Make dataframe containing the sizes
    database_sizes = []

    mysql_size = get_mysql_db_size(host_mysql, username_mysql, password_mysql, f"{db_name_base_mysql}_{name_suffix}")
    mysql_size = convert_bytes_to_gb(mysql_size)
    database_sizes.append({'database': 'MySQL', 'size': mysql_size})

    mongodb_size = get_mongodb_db_size(f"mongodb://{host_mongodb}:{port_mongodb}/", f"{db_name_base_mongodb}_{name_suffix}")
    mongodb_size = convert_bytes_to_gb(mongodb_size)
    database_sizes.append({'database': 'MongoDB', 'size': mongodb_size})

    postgres_size = get_postgres_db_size(host_postgresql, username_postgresql, password_postgresql, f"{db_name_base_postgresql}_{name_suffix}")
    postgres_size = convert_bytes_to_gb(postgres_size)
    database_sizes.append({'database': 'PostgreSQL', 'size': postgres_size})

    sqlite_size = get_sqlite_db_size(f"../{folder_db_sqlite}/{db_name_base_sqlite}_{name_suffix}.db")
    sqlite_size = convert_bytes_to_gb(sqlite_size)
    database_sizes.append({'database': 'SQLite', 'size': sqlite_size})

    # Make plot
    plot_databases_sizes(pd.DataFrame(database_sizes), name_suffix)
