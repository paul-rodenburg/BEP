import tracemalloc
import sqlite3
import pandas as pd

tracemalloc.start()

conn = sqlite3.connect('databases/reddit_data_ALL.db')

q = 'SELECT id FROM comment LIMIT 10000'

df = pd.read_sql_query(q, conn)
print(df)


current, peak = tracemalloc.get_traced_memory()
print(f"Current memory usage: {current / 1024 / 1024:.2f} MB")
print(f"Peak memory usage: {peak / 1024 / 1024:.2f} MB")

tracemalloc.stop()
