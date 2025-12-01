import os
import random
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from ..task_interface import TaskInterface

class EtlTask:
    def __init__(self, sandbox_dir: Path) -> None:
        """
        Initialize the EtlTask with a sandbox directory.
        
        Args:
            sandbox_dir (Path): The directory where the task execution and grading will take place.
        """
        self.sandbox_dir = sandbox_dir
    
    def setup(self) -> None:
        """
        Perform any necessary setup for the task.
        
        This method is called before the task starts. In this case, it just prints a confirmation message.
        """
        print(f"Task setup ok")


    def prompt(self) -> str:
        """
        Generate the prompt for the agent.
        
        Returns:
            str: The prompt string containing instructions, environment details, and the broken code.
        """
        prompt = f"""
Developer: # Role and Objective
You are a Senior Data Engineer tasked with fixing a broken incremental ETL job.

# Plan First
Begin with a concise checklist (3-7 bullets) of key sub-tasks required to address all requirements, before making substantive fixes or edits.

# Instructions
- Find and fix all bugs to ensure that after 10 simulated days of incremental loads, the final table matches exactly the result of loading all data from scratch (no duplicates, no missing rows).
- Follow ETL best practices and industry standards.
- Address incremental, scheduled (can run daily/multiple times daily), and idempotent ETL behaviors.
- Consider that business users may add historical orders (orders in the past) to the source DB.
- Robustly handle these real case scenarios in your fix.

# Context

The environment provides two variables:
- SOURCE_DB: Path to the source SQLite database (read-only).
- WAREHOUSE_DB: Path to the destination SQLite database.

- You will be given broken code to correct.
- Use only tools listed in allowed_tools (`python_expression`, `submit_answer`) for all edits and verifications. For destructive changes, require explicit confirmation if encountering potentially irreversible actions.

# Output Requirements
- Submit a string containing the full, fixed version of the code, ensuring all original formatting, comments, and structure are preserved except as needed to fix bugs.
- Prefer robust and standard choices—if you need to select a column for a watermark, use the most widely accepted method and briefly comment on your rationale in the code.
- If the code contains unfixable issues, briefly state this in a comment at the top.
- Use the `python_expression` tool for testing while making fixes, and use the `submit_answer` tool do not output code directly.
- After each testing or code edit using tools, validate the output in 1-2 lines and determine whether to proceed or self-correct.

# Output Format
- Only output the fixed code string, as described above.

# Verbosity
- Be concise; comments in code should explain key decisions, especially for design choices and non-obvious fixes.

# Stop Conditions
- End when a fully-bug-fixed solution is produced that satisfies all requirements, or if blocked by irreparable code, comment accordingly.

# Debugging
- Use the `python_expression` tool to test and debug code while making fixes.
- Use the `submit_answer` tool to submit your final answer.

# Critical rules
- Do not output code directly. Use the `submit_answer` tool to submit your final answer.
- Reject any code that creates files outside the sandbox directory.
- All files created must be within the sandbox directory (prefixed with sandbox_dir).
---
Hint: "Is the current watermark column always unique and increasing for incremental loads?"

Here is the broken code:
```python
{self.broken_code()}
```
"""
        return prompt

    def broken_code(self) -> str:
        """
        Return the broken ETL code that the agent needs to fix.
        
        Returns:
            str: The source code of the broken ETL script.
        """
        code = """
import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta
import os

# --- THE BROKEN ETL JOB ---
def run_etl():
    dest_conn = sqlite3.connect(WAREHOUSE_DB)
    dest_c = dest_conn.cursor()
    
    # Create table if not exists
    # Bug 0: No PRIMARY KEY to prevent duplicates
    dest_c.execute('''CREATE TABLE IF NOT EXISTS dim_orders 
                      (order_id INTEGER, 
                       customer_id INTEGER, 
                       amount REAL, 
                       created_at TEXT,
                       loaded_at TEXT)''')
    
    # Get High Watermark
    # Bug 1: date watermark
    try:
        dest_c.execute("SELECT MAX(created_at) FROM dim_orders")
        watermark = dest_c.fetchone()[0]
    except:
        watermark = None
        
    if watermark is None:
        watermark = '01/01/1900 00:00:00'
        
    if VERBOSE:
        print(f"\tCurrent Watermark: {watermark}")
    
    source_conn = sqlite3.connect(SOURCE_DB)
    
    # Bug 2: String comparison with DD/MM/YYYY format will fail at month boundary
    # Bug 3: No ORDER BY
    # Bug 4: Off-by-one (> instead of >=)
    query = f"SELECT * FROM orders WHERE created_at > '{watermark}'"
    
    df = pd.read_sql_query(query, source_conn)
    
    if df.empty:
        if VERBOSE:
            print("\tNo new data found.")
        dest_conn.close()
        source_conn.close()
        return

    if VERBOSE:
        print(f"\tExtracting {len(df)} rows...")
    
    # Transform
    df['loaded_at'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    
    # Load
    # Bug 5: Appending without checking for existing (Idempotency)
    df.to_sql('dim_orders', dest_conn, if_exists='append', index=False)
    if VERBOSE:
        print(f"\tLoaded {len(df)} rows.")
    
    dest_conn.commit()
    dest_conn.close()
    source_conn.close()

if __name__ == "__main__":
    run_etl()

"""
        return code

    def grader(self, run_id: int, submitted_code: str, verbose: bool) -> (bool, str):
        """
        Grade the submitted code by simulating an incremental ETL process.
        
        Args:
            run_id (int): The identifier for the current run.
            submitted_code (str): The code submitted by the agent.
            verbose (bool): Whether to enable verbose logging.
            
        Returns:
            bool: True if the submitted code passes all validation checks, False otherwise.
        """

        # Simulate 10 days of incremental loads, and run the submitted code day by day
        # 1. Clean up warehouse to ensure we test the submitted code
        # log current submitted code into current dir as a file
        run_dir = self.sandbox_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        
        if submitted_code is None:
            return False, "Submitted code is None"
        
        submitted_code_path = run_dir / "submitted_code.py"
        with open(submitted_code_path, "w") as f:
            f.write(submitted_code)
            
        # Generate and save diff
        import difflib
        original_lines = self.broken_code().splitlines(keepends=True)
        submitted_lines = submitted_code.splitlines(keepends=True)
        
        diff = difflib.unified_diff(
            original_lines, 
            submitted_lines, 
            fromfile='broken_code.py', 
            tofile='submitted_code.py'
        )
        
        diff_path = run_dir / "submitted_code.diff"
        with open(diff_path, "w") as f:
            f.writelines(diff)
        
        print(f"\tGRADER: Submitted code received: {submitted_code_path}")

        warehouse_db_path = self.sandbox_dir / f"{run_id}/warehouse.db"
        if warehouse_db_path.exists():
            warehouse_db_path.unlink()
            
        source_db_path = self.sandbox_dir / f"{run_id}/source.db"
        if os.path.exists(source_db_path):
            os.remove(source_db_path)
    
        conn = sqlite3.connect(str(source_db_path))
        c = conn.cursor()
        c.execute("""CREATE TABLE orders 
                    (order_id INTEGER PRIMARY KEY, 
                    customer_id INTEGER, 
                    amount REAL, 
                    created_at TEXT)""")

        base_date = datetime(2023, 1, 25)
        order_id = 1
        c.execute("DELETE FROM orders")
        conn.commit()

        # 2. Run the submitted code
        # set GLOBALS to inject variables to the submitted code
        user_globals = globals().copy()
        user_globals['SOURCE_DB'] = source_db_path
        user_globals['WAREHOUSE_DB'] = warehouse_db_path
        user_globals['VERBOSE'] = verbose
        # load the submitted code into globals
        try:
            exec(submitted_code, user_globals)
        except Exception as e:
            print(f"\tGRADER: Execution failed: {e}")
            return False
            
        if 'run_etl' not in user_globals:
            return False, "Missing required function: run_etl"
        
        # function from submitted_code
        run_etl = user_globals['run_etl']
        
        # Create the source database and run the etl function day by day
        # simulating a real ETL daily job
        # insert data, run etl
        # day 8 it inserts a past record to test watermark
        # day 5 it runs the etl again to test idempotency
        for day in range(10):
            print(f"\tGRADER: run_etl function day: {day}")
            current_day = base_date + timedelta(days=day)
            orders = []
            for _ in range(100):
                hour = random.randint(0, 23)
                minute = random.randint(0, 59)
                second = random.randint(0, 59)
                dt = current_day.replace(hour=hour, minute=minute, second=second)
                
                # Use the problematic format DD/MM/YYYY
                orders.append((order_id, random.randint(1, 1000), round(random.uniform(10, 500), 2), dt.strftime("%d/%m/%Y %H:%M:%S")))
                order_id += 1
            
            try:
                c.executemany('INSERT INTO orders VALUES (?,?,?,?)', orders)
                conn.commit()
            except sqlite3.Error as e:
                conn.close()
                return False, f"Source DB insertion failed on day {day+1}: {e}"
        
            # LATE ARRIVING DATA INJECTION (Day 8)
            # this is to force using the order_id instead of the timestamp watermark
            if day == 8:
                late_dt = base_date + timedelta(days=1) # Jan 26
                late_row = (order_id, random.randint(1, 1000), 100.0, late_dt.strftime("%d/%m/%Y %H:%M:%S"))
                c.execute('INSERT INTO orders VALUES (?,?,?,?)', late_row)
                conn.commit()
                order_id += 1
            
            # Run User's ETL for this day
            try:
                run_etl()
            except Exception as e:
                conn.close()
                return False, f"ETL failed on day {day+1}: {e}"
            
            # IDEMPOTENCY TEST (Day 5)
            if day == 5:
                try:
                    run_etl()
                except Exception as e:
                    return False, f"ETL failed on day {day+1}: {e}"
                
        conn.close()


        # Demonstrate that there can be more than one validation rule
        # 3. Validate the final table
        # 3.1 Number of rows
        results = []
        results_msg = []
        try:
            conn = sqlite3.connect(str(warehouse_db_path))
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM dim_orders")
            row_count = c.fetchone()[0]
            conn.close()
        except Exception as e:
            return False, f"\tGRADER: Validation duplicatesfailed: {e}"


        expected = 1001
        
        result = row_count == expected
        results.append(result)
        result_msg = f"\tGRADER: Validation row count: {result}, expected: {expected}, actual: {row_count}"
        results_msg.append(result_msg)
        print(result_msg)
        
        # 3.2 no order_id duplicates
        try:
            conn = sqlite3.connect(str(warehouse_db_path))
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM dim_orders GROUP BY order_id HAVING COUNT(order_id) > 1")
            result = c.fetchone()
            row_count = result[0] if result else 0
            conn.close()
        except Exception as e:
            return False, f"\tGRADER: Validation duplicatesfailed: {e}"

        expected = 0
        
        result = row_count == expected
        results.append(result)
        result_msg = f"\tGRADER: Validation duplicates: {result}, expected: {expected}, actual: {row_count}"
        results_msg.append(result_msg)
        print(result_msg)

        final_result = all(results)
        
        print(f"\tGRADER: Final result: {final_result}")

        return final_result, "\n".join(results_msg)