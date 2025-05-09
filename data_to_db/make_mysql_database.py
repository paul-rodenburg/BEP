from sqlalchemy import text
import os
from classes.DBType import DBType, DBTypes
from data_to_sql import main
from general import check_files, make_mysql_engine, load_json

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

# Make 'databases' folder for SQLite database and .json file containing info about each database
os.makedirs('databases', exist_ok=True)

DB_NAME = load_json('config.json')['mysql']['db_name']
db_type_mysql = DBType(db_type=DBTypes.MYSQL, name='20m', max_rows=20_000_000)

# Make engine (set db_type to None because it can be that the database doesn't exist yet)
engine = make_mysql_engine(db_type=None)

# Check if necessary data files exist
check_files(db_type=db_type_mysql)

# Create a new database
with engine.connect() as conn:
    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {DB_NAME}"))
    conn.commit()

# Make engine again if the database needed to be created
engine = make_mysql_engine(db_type_mysql)
main(engine, db_type_mysql)
