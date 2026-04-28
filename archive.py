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
    'database': 'bdfunnelbuilder'
}

table_name = 'orders'
batch_size = 1000  # Number of rows to move per batch

# --- Logging Setup ---
logging.basicConfig(
    filename='transfer_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def transfer_large_data():
    src_conn = None
    dest_conn = None
    total_moved = 0

    try:
        # Establish connections
        src_conn = mysql.connector.connect(**source_config)
        dest_conn = mysql.connector.connect(**dest_config)
        # buffered=True allows us to keep the read cursor open during inserts
        src_cursor = src_conn.cursor(buffered=True)
        dest_cursor = dest_conn.cursor()
        # 1. Pre-transfer Verification
        src_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        # dest_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 0;")
        source_total = src_cursor.fetchone()[0]
        logging.info(f"START: Found {source_total} rows in {table_name}")
        print(f"Starting transfer of {source_total} rows...")
        # 2. Open Stream
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
                print(rows)

            try:
                dest_cursor.executemany(insert_sql, rows)
                dest_conn.commit()
                total_moved += len(rows)
                print(f"Progress: {total_moved}/{source_total} moved...", end='\r')
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
        if src_conn: src_conn.close()
        if dest_conn: dest_conn.close()

if __name__ == '__main__':
    transfer_large_data()
