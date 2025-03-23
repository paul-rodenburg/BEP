from sqlalchemy import create_engine
from data_to_sql import process_data
import os

os.makedirs('databases', exist_ok=True)

# PostgreSQL connection setup
DB_NAME = "reddit_data"
DB_USER = "postgres"
DB_PASSWORD = "admin"
DB_HOST = "localhost"  # Change if using a remote database
DB_PORT = "5432"

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

process_data(engine)
