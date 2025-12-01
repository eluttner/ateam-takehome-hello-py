import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# --- THE FIXED ETL JOB ---
def run_etl():
    """
    Fixed incremental ETL with proper idempotency and historical data handling.
    
    Key fixes applied:
    0. Added PRIMARY KEY UNIQUE on order_id to prevent duplicate rows
    1. Changed watermark format to ISO (YYYY-MM-DD HH:MM:SS) for reliable comparison
    2. Added 7-day lookback window to catch historical orders added to source
    3. Added ORDER BY clause for deterministic processing
    4. Implemented delete-before-insert pattern for idempotency
    5. Fixed numpy int type issue when building delete query
    """
    dest_conn = sqlite3.connect(WAREHOUSE_DB)
    dest_c = dest_conn.cursor()
    
    # FIX 0: PRIMARY KEY UNIQUE prevents duplicate order_ids
    dest_c.execute('''CREATE TABLE IF NOT EXISTS dim_orders 
                      (order_id INTEGER PRIMARY KEY UNIQUE, 
                       customer_id INTEGER, 
                       amount REAL, 
                       created_at TEXT,
                       loaded_at TEXT)''')
    
    # FIX 1: Use ISO format (YYYY-MM-DD HH:MM:SS) for reliable watermark comparison
    # ISO format enables lexicographic ordering and avoids ambiguity
    try:
        dest_c.execute("SELECT MAX(created_at) FROM dim_orders")
        watermark = dest_c.fetchone()[0]
    except:
        watermark = None
        
    if watermark is None:
        watermark_dt = datetime(1900, 1, 1)
        # FIX 2: Lookback window to catch historical orders added to source
        lookback_dt = watermark_dt
    else:
        watermark_dt = datetime.fromisoformat(watermark)
        # Look back 7 days to catch any retroactively added historical orders
        lookback_dt = watermark_dt - timedelta(days=7)
        
    if VERBOSE:
        print(f"\tCurrent Watermark: {watermark}")
        print(f"\tLookback Window: {lookback_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    
    source_conn = sqlite3.connect(SOURCE_DB)
    
    # FIX 3, 4: Use > (not >=) to avoid boundary issues, and add ORDER BY for determinism
    # Use ISO format in query
    query = f"""SELECT * FROM orders 
                WHERE created_at > '{lookback_dt.strftime('%Y-%m-%d %H:%M:%S')}' 
                ORDER BY created_at, order_id"""
    
    df = pd.read_sql_query(query, source_conn)
    source_conn.close()
    
    if df.empty:
        if VERBOSE:
            print("\tNo new data found.")
        dest_conn.close()
        return

    if VERBOSE:
        print(f"\tExtracting {len(df)} rows...")
    
    # FIX 5: Delete before insert ensures idempotency and handles historical data
    # This prevents duplicates on re-runs and catches orders added retroactively
    if len(df) > 0:
        # Convert to Python int to avoid numpy type issues with SQL parameter binding
        order_ids = tuple(int(x) for x in df['order_id'].values)
        placeholders = ','.join('?' * len(order_ids))
        delete_query = f"DELETE FROM dim_orders WHERE order_id IN ({placeholders})"
        dest_c.execute(delete_query, order_ids)
        deleted_count = dest_c.rowcount
        if VERBOSE and deleted_count > 0:
            print(f"\tDeleted {deleted_count} existing records for re-processing")
    
    # Transform
    df['loaded_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Load
    df.to_sql('dim_orders', dest_conn, if_exists='append', index=False)
    if VERBOSE:
        print(f"\tLoaded {len(df)} rows.")
    
    dest_conn.commit()
    dest_conn.close()

if __name__ == "__main__":
    run_etl()