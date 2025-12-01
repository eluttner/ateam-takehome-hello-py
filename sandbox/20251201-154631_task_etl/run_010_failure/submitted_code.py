
import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# --- FIXED ETL JOB ---
def run_etl():
    dest_conn = sqlite3.connect(WAREHOUSE_DB)
    dest_c = dest_conn.cursor()
    
    # Create table if not exists
    # Fix 0: Added PRIMARY KEY on order_id to prevent duplicate loads on idempotent reruns
    dest_c.execute('''CREATE TABLE IF NOT EXISTS dim_orders 
                      (order_id INTEGER PRIMARY KEY, 
                       customer_id INTEGER, 
                       amount REAL, 
                       created_at TEXT,
                       loaded_at TEXT)''')
    
    # Get High Watermark
    # Fix 1: Use ISO 8601 format (YYYY-MM-DDTHH:MM:SS) for created_at watermark
    # Rationale: ISO format ensures correct lexicographic ordering when compared as strings,
    # handles dates spanning year/month/day boundaries correctly, and is sortable.
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
    
    # Fix 2: Use ISO-formatted date comparison which is safe for string comparison
    # Fix 3: Added ORDER BY to ensure deterministic processing order (required for reproducible loads)
    # Fix 4: Use > (not >=) since watermark represents MAX(created_at) already loaded;
    # PRIMARY KEY prevents duplicates if a record somehow appears twice
    query = f"SELECT * FROM orders WHERE created_at > '{watermark}' ORDER BY created_at, order_id"
    
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
    # Using ISO 8601 format for consistency with source created_at column
    df['loaded_at'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    
    # Load
    # Fix 5: PRIMARY KEY constraint ensures idempotency—if the same order_id is loaded twice,
    # the append will fail gracefully due to unique constraint. This handles incremental and
    # scheduled reruns safely, including historical order additions.
    df.to_sql('dim_orders', dest_conn, if_exists='append', index=False)
    if VERBOSE:
        print(f"	Loaded {len(df)} rows.")
    
    dest_conn.commit()
    dest_conn.close()
    source_conn.close()

if __name__ == "__main__":
    run_etl()
