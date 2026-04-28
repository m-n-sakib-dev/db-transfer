import mysql.connector
import argparse
import sys
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

target_shops = []

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

        params = [archive_date]

        if table_name in archive_tables:
            query = f"SELECT * FROM {table_name} WHERE created_at < %s"
            if target_shops:
                placeholders = ', '.join(['%s'] * len(target_shops))
                query += f" AND shop_id IN ({placeholders})"
                params.extend(target_shops)
            src_cursor.execute(query, tuple(params))

        elif table_name in order_dependent_archive_table:
            query = (f"SELECT {table_name}.* FROM {table_name} "
                     f"INNER JOIN orders ON {table_name}.order_id = orders.id "
                     f"WHERE orders.created_at < %s")
            if target_shops:
                placeholders = ', '.join(['%s'] * len(target_shops))
                query += f" AND orders.shop_id IN ({placeholders})"
                params.extend(target_shops)

            src_cursor.execute(query, tuple(params))

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
                logging.info(f"Progress: {total_moved}/{source_total} moved...")
            except mysql.connector.Error as e:
                logging.error(f"Error inserting batch near row {total_moved}: {e}")

        # 4. Post-transfer Verification
        dest_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_dest_count = dest_cursor.fetchone()[0]
        
        report = f"COMPLETE: Moved {total_moved} rows. Target total: {final_dest_count}"
        logging.info(report)

    except mysql.connector.Error as err:
        logging.critical(f"Connection failed: {err}")
    finally:
        disconnect_db(src_conn, dest_conn)

        
        
def get_all_tables():
    src_conn = None
    dest_conn = None
    try:
        src_conn , dest_conn = connect_db() 
        src_cursor = src_conn.cursor(buffered=True)
        src_cursor.execute(f"SHOW TABLES;")
        rows = src_cursor.fetchall()
        tables_name = [r[0] for r in rows]
        tables_name.remove('job_batches')
        return tables_name
        
    except mysql.connector.Error as err:
        logging.critical(f"Connection failed: {err}")
    finally:
        disconnect_db(src_conn, dest_conn)        
        
        
def transfer_data(target_tables):
    tables_name = []
    try:
        if not target_tables:
            tables_name = get_all_tables()
        else :
            tables_name = target_tables
        if 'job_batches' in tables_name:
            tables_name.remove('job_batches')
        for t_name in tables_name:
            transfer_table_data(t_name) 
        
    except mysql.connector.Error as err:
        logging.critical(f"Connection failed: {err}")
        

def get_args():
    parser = argparse.ArgumentParser(description="Database Archiving Script")

    # Mandatory Arguments
    parser.add_argument("--src_host", required=True, help="Source Host IP")
    parser.add_argument("--src_port", required=True, help="Source Host port")
    parser.add_argument("--src_user", required=True, help="Source Host User")
    parser.add_argument("--src_password", required=True, help="Source Host Password")
    parser.add_argument("--src_db", required=True, help="Source Database name")
    
    
    parser.add_argument("--archive_host", required=True, help="Archive Host IP")
    parser.add_argument("--archive_port", required=True, help="Archive Host port")
    parser.add_argument("--archive_user", required=True, help="Archive Host User")
    parser.add_argument("--archive_password", required=True, help="Archive Host Password")
    parser.add_argument("--archive_db", required=True, help="Archive Database name")
    
    parser.add_argument("--start_date", required=True, help="Start Date (YYYY-MM-DD)")

    # Optional Arguments (Defaults to empty list/None)
    parser.add_argument("--tables", nargs='*', default=[], help="Specific tables (space separated). Leave empty for all.")
    parser.add_argument("--shop_ids", nargs='*', default=[], help="Specific Shop IDs (space separated). Leave empty for all.")

    return parser.parse_args()


def main():
    args = get_args()
    # Accessing variables
    # Updating global source_config dictionary
    source_config['host'] = args.src_host
    source_config['port'] = int(args.src_port) if args.src_port else 3306
    source_config['user'] = args.src_user
    source_config['password'] = args.src_password
    source_config['database'] = args.src_db

    # Updating global dest_config dictionary
    dest_config['host'] = args.archive_host
    dest_config['port'] = int(args.archive_port) if args.archive_port else 3306
    dest_config['user'] = args.archive_user
    dest_config['password'] = args.archive_password
    dest_config['database'] = args.archive_db

    # Process optional lists
    # If user leaves empty, these will be an empty list []
    tables_to_move = args.tables
    all_tables = get_all_tables()
    for table in tables_to_move:
        if table not in all_tables:
            logging.error(f"Error: {table} does not exists in database.")
            sys.exit(1)
        
    global target_shops
    target_shops = args.shop_ids
    # Date Validation
    try:
        archive_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    except ValueError:
        logging.error("Error: Dates must be in YYYY-MM-DD format.")
        sys.exit(1)
    
    transfer_data(tables_to_move)
    # print(f"source_config: {source_config}")
    # print(f"dest_config: {dest_config}")
    # print(f"tables_to_move: {tables_to_move}")
    # print(f"shops_to_move: {target_shops }")
    # print(f"archive_date: {archive_date}")



if __name__ == '__main__':
    main()
