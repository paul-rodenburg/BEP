from data_to_sql import main, load_json
import os
from general import check_files, make_sqlite_engine
from classes.DBType import DBType, DBTypes

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

# Make 'databases' folder for SQLite database and .json file containing info about each database
os.makedirs('databases', exist_ok=True)
db_folder = load_json('config.json')['sqlite']['db_folder']
os.makedirs(db_folder, exist_ok=True)

# Check if necessary data files exist
check_files()

db_type = DBType(db_type=DBTypes.SQLITE, name='20m', max_rows=20_000_000)
engine = make_sqlite_engine(db_type)
main(engine, db_type)
