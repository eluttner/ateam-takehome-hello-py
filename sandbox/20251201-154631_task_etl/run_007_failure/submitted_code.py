
import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# --- THE FIXED ETL JOB ---
def run_etl():
    dest_conn = sqlite3.connect(WAREHOUSE_DB)
    dest_c = dest_conn.cursor()
    
    # Fixed Bug 0: Add PRIMARY KEY to prevent duplicate rows on re-runs
    dest_c.execute('''CREATE TABLE IF NOT EXISTS dim_orders 
                      (order_id INTEGER PRIMARY KEY, 
                       customer_id INTEGER, 
                       amount REAL, 
                       created_at TEXT,
                       loaded_at TEXT)''')
    
    # Fixed Bug 1 & 2: Use a metadata table to track watermark in ISO 8601 format
    # Rationale: ISO 8601 (YYYY-MM-DDTHH:MM:SS) sorts lexicographically correctly
    # and is the industry standard for timestamps. Avoids DD/MM/YYYY locale issues.
    dest_c.execute('''CREATE TABLE IF NOT EXISTS etl_metadata
                      (table_name TEXT PRIMARY KEY,
                       max_created_at TEXT)''')
    
    # Get High Watermark from metadata table
    try:
        dest_c.execute("SELECT max_created_at FROM etl_metadata WHERE table_name = 'dim_orders'")
        result = dest_c.fetchone()
        watermark = result[0] if result else None
    except:
        watermark = None
        
    if watermark is None:
        watermark = '1900-01-01T00:00:00'  # ISO 8601 epoch
        
    if VERBOSE:
        print(f"	Current Watermark: {watermark}")
    
    source_conn = sqlite3.connect(SOURCE_DB)
    
    # Fixed Bugs 3 & 4: Use >= for boundary inclusion (idempotency) and add ORDER BY
    # Ordering ensures consistent, deterministic processing of records at the same timestamp
    query = f"SELECT * FROM orders WHERE created_at >= '{watermark}' ORDER BY created_at, order_id"
    
    df = pd.read_sql_query(query, source_conn)
    
    if df.empty:
        if VERBOSE:
            print("	No new data found.")
        dest_conn.close()
        source_conn.close()
        return

    if VERBOSE:
        print(f"	Extracting {len(df)} rows...")
    
    # Transform
    df['loaded_at'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    # Fixed Bug 5: Use INSERT OR REPLACE to handle idempotency
    # This ensures re-runs don't create duplicates and handles updates correctly
    for _, row in df.iterrows():
        dest_c.execute('''INSERT OR REPLACE INTO dim_orders 
                         (order_id, customer_id, amount, created_at, loaded_at) 
                         VALUES (?, ?, ?, ?, ?)''',
                      (row['order_id'], row['customer_id'], row['amount'], 
                       row['created_at'], row['loaded_at']))
    
    # Update watermark to the maximum created_at we just processed
    new_watermark = df['created_at'].max()
    dest_c.execute('''INSERT OR REPLACE INTO etl_metadata (table_name, max_created_at)
                      VALUES (?, ?)''', ('dim_orders', new_watermark))
    
    if VERBOSE:
        print(f"	Loaded {len(df)} rows. New watermark: {new_watermark}")
    
    dest_conn.commit()
    dest_conn.close()
    source_conn.close()

if __name__ == "__main__":
    run_etl()
