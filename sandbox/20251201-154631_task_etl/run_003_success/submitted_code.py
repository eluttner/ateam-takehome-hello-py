import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# --- FIXED ETL JOB ---
def run_etl():
    dest_conn = sqlite3.connect(WAREHOUSE_DB)
    dest_c = dest_conn.cursor()
    
    # FIX 0: Added PRIMARY KEY constraint on order_id to prevent duplicate rows.
    # This ensures uniqueness at the database level, preventing multiple loads
    # of the same order_id from creating duplicates.
    dest_c.execute('''CREATE TABLE IF NOT EXISTS dim_orders 
                      (order_id INTEGER PRIMARY KEY, 
                       customer_id INTEGER, 
                       amount REAL, 
                       created_at TEXT,
                       loaded_at TEXT)''')
    
    # FIX 1 & 6: Changed from timestamp-based watermarking to order_id-based watermarking.
    # Rationale: order_id is guaranteed unique and monotonically increasing.
    # This is more robust than created_at timestamps because:
    #   - Handles historical data insertions (orders added for past dates)
    #   - Unaffected by timestamp duplicates (multiple orders at same second)
    #   - Unaffected by system clock changes or timezone issues
    # We use MAX(order_id) as the watermark, retrieving all orders with ID > watermark.
    try:
        dest_c.execute("SELECT COALESCE(MAX(order_id), 0) FROM dim_orders")
        watermark_id = dest_c.fetchone()[0]
    except:
        watermark_id = 0
        
    if VERBOSE:
        print(f"\tCurrent Watermark (Max Order ID): {watermark_id}")
    
    source_conn = sqlite3.connect(SOURCE_DB)
    
    # FIX 2, 3, 4: Changed watermark logic:
    #   - Removed DD/MM/YYYY format (lexicographic comparison fails at month boundaries)
    #   - Added ORDER BY for deterministic result ordering
    #   - Using > comparison (order_id is unique, never re-process same ID)
    # Using order_id guarantees deterministic behavior independent of timestamp ordering.
    query = f"""SELECT * FROM orders 
                WHERE order_id > {watermark_id} 
                ORDER BY order_id ASC"""
    
    df = pd.read_sql_query(query, source_conn)
    
    if df.empty:
        if VERBOSE:
            print("\tNo new data found.")
        dest_conn.close()
        source_conn.close()
        return

    if VERBOSE:
        print(f"\tExtracting {len(df)} rows...")
    
    # Transform: Add loaded_at timestamp
    df['loaded_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # FIX 5: Implemented idempotent load using DELETE+INSERT pattern.
    # This ensures that if the ETL runs multiple times with overlapping data,
    # no duplicates are created. The pattern:
    #   1. Delete all rows with order_ids in the current batch
    #   2. Insert the batch fresh
    # This guarantees idempotency: running the same load multiple times 
    # produces identical results as running it once.
    new_ids = tuple(df['order_id'].tolist())
    if len(new_ids) > 0:
        placeholders = ','.join('?' * len(new_ids))
        dest_c.execute(f"DELETE FROM dim_orders WHERE order_id IN ({placeholders})", new_ids)
    
    df.to_sql('dim_orders', dest_conn, if_exists='append', index=False)
    
    if VERBOSE:
        print(f"\tLoaded {len(df)} rows.")
    
    dest_conn.commit()
    dest_conn.close()
    source_conn.close()

if __name__ == "__main__":
    run_etl()
