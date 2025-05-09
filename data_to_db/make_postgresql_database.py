import os
from data_to_sql import main
from general import check_files, make_postgres_engine
from classes.DBType import DBType, DBTypes

# Update working directory
current_directory = os.getcwd()
parent_directory = os.path.dirname(current_directory)
os.chdir(parent_directory)

# Make 'databases' folder for SQLite database and .json file containing info about each database
os.makedirs('databases', exist_ok=True)

# Check if necessary data files exist
check_files()

# Make engine
db_type_postgresql = DBType(db_type=DBTypes.POSTGRESQL, name='ALL')
engine = make_postgres_engine()

main(engine, db_type_postgresql)

