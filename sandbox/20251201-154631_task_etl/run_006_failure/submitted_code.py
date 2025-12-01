
import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# --- FIXED ETL JOB ---
def run_etl():
    dest_conn = sqlite3.connect(WAREHOUSE_DB)
    dest_c = dest_conn.cursor()
    
    # Fix 0: Add PRIMARY KEY on order_id to prevent duplicates and enable UPSERT idempotency
    dest_c.execute('''CREATE TABLE IF NOT EXISTS dim_orders 
                      (order_id INTEGER PRIMARY KEY, 
                       customer_id INTEGER, 
                       amount REAL, 
                       created_at TEXT,
                       loaded_at TEXT)''')
    
    # Fix 1: Use ISO 8601 timestamp format for watermark (lexicographically sortable and reliable)
    # ISO 8601 is the standard for timestamp comparisons and sorts correctly as strings
    try:
        dest_c.execute("SELECT MAX(created_at) FROM dim_orders")
        watermark = dest_c.fetchone()[0]
    except:
        watermark = None
        
    if watermark is None:
        watermark = '1900-01-01T00:00:00'
        
    if VERBOSE:
        print(f"	Current Watermark: {watermark}")
    
    source_conn = sqlite3.connect(SOURCE_DB)
    
    # Fix 2, 3, 4: Use >= for inclusivity at boundaries, add ORDER BY for determinism,
    # use parameterized query to prevent SQL injection, assume source data in ISO format
    query = "SELECT * FROM orders WHERE created_at >= ? ORDER BY created_at, order_id"
    
    df = pd.read_sql_query(query, source_conn, params=(watermark,))
    
    if df.empty:
        if VERBOSE:
            print("	No new data found.")
        dest_conn.close()
        source_conn.close()
        return

    if VERBOSE:
        print(f"	Extracting {len(df)} rows...")
    
    # Transform: Use ISO 8601 format for consistency and reliability
    df['loaded_at'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    # Fix 5: Use INSERT OR REPLACE (UPSERT) to achieve idempotency
    # This handles re-runs without duplicates and supports historical inserts
    # PRIMARY KEY on order_id ensures only one version of each order exists
    for _, row in df.iterrows():
        dest_c.execute('''INSERT OR REPLACE INTO dim_orders 
                          (order_id, customer_id, amount, created_at, loaded_at)
                          VALUES (?, ?, ?, ?, ?)''',
                       (row['order_id'], row['customer_id'], row['amount'], 
                        row['created_at'], row['loaded_at']))
    
    if VERBOSE:
        print(f"	Loaded {len(df)} rows.")
    
    dest_conn.commit()
    dest_conn.close()
    source_conn.close()

if __name__ == "__main__":
    run_etl()
