import sqlite3
import pandas as pd
from general import get_primary_key, get_tables_database
from tqdm import tqdm

tables = get_tables_database()
conn = sqlite3.connect('data.db')

for table in tqdm(tables, desc='Checking SQL Tables'):
    if table in ['banned', 'removed']:
        continue
    pm = get_primary_key(table)
    q = f"SELECT {pm} FROM {table}"
    df = pd.read_sql_query(q, conn)
    len_before = len(df)
    df = df.drop_duplicates(subset=pm)
    len_after = len(df)
    if len_after != len_before:  # Primary key is wrong
        print(f'PM in table {table} is wrong! ({len_before:,} -> {len_after:,})')


conn.close()
