import sqlite3
from data_to_sql import process_data
import os

os.makedirs('databases', exist_ok=True)
conn = sqlite3.connect('databases/reddit_data.db')

process_data(conn)
