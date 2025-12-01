import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# --- FIXED ETL JOB ---
def run_etl():
    dest_conn = sqlite3.connect(WAREHOUSE_DB)
    dest_c = dest_conn.cursor()
    
    # FIX 0: Added PRIMARY KEY on order_id to prevent duplicates
    dest_c.execute('''CREATE TABLE IF NOT EXISTS dim_orders 
                      (order_id INTEGER PRIMARY KEY, 
                       customer_id INTEGER, 
                       amount REAL, 
                       created_at TEXT,
                       loaded_at TEXT)''')
    
    # FIX 1: Use ISO format (YYYY-MM-DDTHH:MM:SS) for watermark
    # Rationale: ISO format is lexicographically sortable and handles all edge cases
    # (no month boundary issues with DD/MM/YYYY string comparison)
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
    
    # FIX 2-3: Use ISO format comparison + add ORDER BY for deterministic ordering
    # ORDER BY created_at, order_id ensures tie-breaking consistency
    query = f"""SELECT * FROM orders 
                WHERE created_at > '{watermark}' 
                ORDER BY created_at, order_id"""
    
    df = pd.read_sql_query(query, source_conn)
    
    # FIX 4: Detect late-arriving historical data by checking for new order_ids
    # This handles business user scenario: historical orders added to source DB
    source_c = source_conn.cursor()
    source_c.execute("""SELECT DISTINCT order_id FROM orders""")
    source_order_ids = {row[0] for row in source_c.fetchall()}
    
    dest_c.execute("""SELECT DISTINCT order_id FROM dim_orders""")
    warehouse_order_ids = {row[0] for row in dest_c.fetchall()}
    
    missing_ids = source_order_ids - warehouse_order_ids
    
    if missing_ids:
        # New order_ids found - fetch them from source
        placeholders = ','.join('?' * len(missing_ids))
        query_missing = f"""SELECT * FROM orders 
                           WHERE order_id IN ({placeholders})
                           ORDER BY created_at, order_id"""
        df_missing = pd.read_sql_query(query_missing, source_conn, params=list(missing_ids))
        df = pd.concat([df, df_missing], ignore_index=True).drop_duplicates(subset=['order_id'], keep='first')
    
    if df.empty:
        if VERBOSE:
            print("	No new data found.")
        dest_conn.close()
        source_conn.close()
        return

    if VERBOSE:
        print(f"	Extracting {len(df)} rows...")
    
    # Transform
    df['loaded_at'] = datetime.now().isoformat()
    
    # FIX 5: Use INSERT OR REPLACE for idempotent upsert
    # Handles: duplicates on rerun, updates to existing orders, late-arriving data
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
