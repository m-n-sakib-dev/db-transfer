import mysql.connector
import logging
from datetime import datetime

# --- Configuration ---
source_config = {
    'host': '127.0.0.1',
    'port': 3306,     
    'user': 'root',
    'password': '',
    'database': 'archive_db_2'
}

dest_config = {
    'host': '127.0.0.1',
    'port': 3306,     
    'user': 'root',
    'password': '',
    'database': 'backupdatabase'
}

batch_size = 10000  # Number of rows to move per batch
archive_date = '2025-07-01 00:00:00'
archive_tables = [
    'activity_log',
    'fake_order_settings',
    'sales',
    'sales_target_histories',
    'sales_targets',
    'call_histories',
    'orders'
    
]

order_dependent_archive_table=[
    'comments',
    'applied_coupons',
    'agent_assign_logs',
    'call_initiation_logs',
    'call_automation_logs',
    'order_items',
    'order_logs',
    'order_metas',

]


# --- Logging Setup ---
logging.basicConfig(
    filename='transfer_log.log',
    filemode='w', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def connect_db():
    try:
        src_conn = mysql.connector.connect(**source_config)
        dest_conn = mysql.connector.connect(**dest_config)
        return [src_conn,dest_conn]
    except mysql.connector.Error as err:
        logging.critical(f"Connection failed: {err}")
        print(f"Critical Error: {err}")
        
def disconnect_db(src_conn,dest_conn):
    if src_conn: src_conn.close()
    if dest_conn: dest_conn.close()

def transfer_table_data(table_name):
    src_conn = None
    dest_conn = None
    total_moved = 0

    try:
        src_conn , dest_conn = connect_db() 
        # buffered=True allows us to keep the read cursor open during inserts
        src_cursor = src_conn.cursor(buffered=True)
        dest_cursor = dest_conn.cursor()
        # 1. Pre-transfer Verification
        src_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")

        source_total = src_cursor.fetchone()[0]
        logging.info(f"START: Found {source_total} rows in {table_name}")
        print(f"Starting transfer of {source_total} rows...")
        # 2. Open Stream
        if table_name in archive_tables:
            src_cursor.execute(f"SELECT * FROM {table_name} WHERE created_at < %s", (archive_date,))
        elif table_name in order_dependent_archive_table:
            src_cursor.execute(f"SELECT {table_name}.*  FROM {table_name} INNER JOIN orders ON {table_name}.order_id = orders.id WHERE orders.created_at < %s", (archive_date,))
        else:
            src_cursor.execute(f"SELECT * FROM {table_name} ")

        # 3. Batch Processing Loop
        while True:
            rows = src_cursor.fetchmany(batch_size)
            if not rows:
                break
            # Setup dynamic SQL placeholders once
            if total_moved == 0:
                placeholders = ', '.join(['%s'] * len(rows[0]))
                insert_sql = f"INSERT IGNORE INTO {table_name} VALUES ({placeholders})"

            try:
                dest_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 0;")
                dest_cursor.executemany(insert_sql, rows)
                dest_conn.commit()
                dest_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 1;")
                total_moved += len(rows)
                print(f"Progress: {total_moved}/{source_total} moved...", end='\r')
                logging.info(f"Progress: {total_moved}/{source_total} moved...")
            except mysql.connector.Error as e:
                logging.error(f"Error inserting batch near row {total_moved}: {e}")

        # 4. Post-transfer Verification
        dest_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_dest_count = dest_cursor.fetchone()[0]
        
        report = f"COMPLETE: Moved {total_moved} rows. Target total: {final_dest_count}"
        logging.info(report)
        print(f"\n{report}")

    except mysql.connector.Error as err:
        logging.critical(f"Connection failed: {err}")
        print(f"Critical Error: {err}")
    finally:
        disconnect_db(src_conn, dest_conn)
        
        
def transfer_data():
    src_conn = None
    dest_conn = None
    total_moved = 0
    try:
        src_conn , dest_conn = connect_db() 
        src_cursor = src_conn.cursor(buffered=True)
        src_cursor.execute(f"SHOW TABLES;")
        rows = src_cursor.fetchall()
        tables_name = [r[0] for r in rows]
        tables_name.remove('job_batches')
        for t_name in tables_name:
            transfer_table_data(t_name) 
        
    except mysql.connector.Error as err:
        logging.critical(f"Connection failed: {err}")
        print(f"Critical Error: {err}")
    finally:
        disconnect_db(src_conn, dest_conn)

if __name__ == '__main__':
    transfer_data()
