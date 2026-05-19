import logging
import mysql.connector
import sqlite3
import logging
from datetime import datetime
import time
import os

# --- Configuration ---
# cron_db_config = {
#     'host': '104.248.157.66',
#     'port': 3306,     
#     'user': 'user_sakib',
#     'password': 'your_password',
#     'database': 'cron_db',
#     'ssl_ca': ""
# }
cron_db_config = {
    'database': 'database/cron_db.db'
}

source_config = {
    'host': '127.0.0.1',
    'port': 3306,     
    'user': 'root',
    'password': '',
    'database': 'archive_db_2',
    'ssl_ca': ""
}

dest_config = {
    'host': '127.0.0.1',
    'port': 3306,     
    'user': 'root',
    'password': '',
    'database': 'backupdatabase',
    'ssl_ca': ""
}

batch_size = 10000  # Number of rows to move per batch
sleep_time = 100
start_date = '2025-07-01 00:00:00'
end_date = '2026-07-01 00:00:00'
delete_source_rows = False
delete_dest_rows = False
archive_tables = [
    'activity_log',
    'sales_target_histories',
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

error_tables = []

target_shops = []
table_not_check_shop_id = ['activity_log', 'sales']

# --- Logging Setup ---
transfer_log = logging.getLogger('transfer_log')
transfer_log.setLevel(logging.INFO)
transfer_log_handler = logging.FileHandler('transfer_log.log',mode='w')
transfer_log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
transfer_log_handler.setFormatter(transfer_log_formatter)
transfer_log.addHandler(transfer_log_handler)

cron_log = logging.getLogger('cron_log')
cron_log.setLevel(logging.INFO)
cron_log_handler = logging.FileHandler('cron_log.log',mode='a')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
cron_log_handler.setFormatter(formatter)
cron_log.addHandler(cron_log_handler)



def connect_db(config):
    try:
        if config == cron_db_config:
            conn = sqlite3.connect(**config)
            return conn
        else:
            conn = mysql.connector.connect(**config)
            return conn
    except mysql.connector.Error as err:
        transfer_log.critical(f"Failed to connect to {config['host']}:{config['database']} : {err}")
        return None
        
def disconnect_db(conn):
    if conn: conn.close()

def sleep():
    if sleep_time  >= 0: 
        transfer_log.info(f"Process is sleeping....")
        time.sleep(sleep_time)
        transfer_log.info(f"Process started again....")


def transfer_table_data(table_name):
    delete_status = False
    pid = os.getpid()
    transfer_log.info(f"-----------------------------------------------------")
    transfer_log.info(f"-----------------------------------------------------")
    
    transfer_log.info(f"Starting transfer for table: {table_name}")
    src_conn = None
    dest_conn = None
    total_moved = 0
    total_deleted = 0

    try:
        src_conn = connect_db(source_config)
        dest_conn = connect_db(dest_config) 
        cron_db_conn = connect_db(cron_db_config)

        src_cursor = src_conn.cursor(buffered=True)        
        dest_cursor = dest_conn.cursor()
        src_delete_cursor = src_conn.cursor()
        cron_db_conn.row_factory = sqlite3.Row
        cron_db_cursor = cron_db_conn.cursor()

        params = [start_date, end_date]

        if table_name in archive_tables:
            query = f"SELECT * FROM {table_name} WHERE created_at BETWEEN %s AND %s"
            if target_shops and table_name not in table_not_check_shop_id:
                placeholders = ', '.join(['%s'] * len(target_shops))
                query += f" AND shop_id IN ({placeholders})"
                params.extend(target_shops)
                
            src_cursor.execute(query, tuple(params))


        elif table_name in order_dependent_archive_table:

            query = (f"SELECT {table_name}.* FROM {table_name} "
                     f"INNER JOIN orders ON {table_name}.order_id = orders.id "
                     f"WHERE orders.created_at BETWEEN %s AND %s")
            if target_shops and table_name not in table_not_check_shop_id:

                placeholders = ', '.join(['%s'] * len(target_shops))
                query += f" AND orders.shop_id IN ({placeholders})"
                params.extend(target_shops)

            src_cursor.execute(query, tuple(params)) 

        else:
            src_cursor.execute(f"SELECT * FROM {table_name} ")

            
        total_rows = src_cursor.rowcount
        transfer_log.info(f"START: Found {total_rows} rows in {table_name}")
        cron_db_cursor.execute("REPLACE INTO process (scheduler_id, table_name, target_total_rows, status) SELECT s.id, ?, ?, ? FROM schedules s WHERE s.process_id = ?", (table_name, total_rows,'active', pid))
        cron_db_conn.commit()
        column_names = [desc[0] for desc in src_cursor.description]     #getting column names of that table
        id_index = column_names.index('id') if 'id' in column_names else None       #getting the index to id column

        try:
            while True:
                cron_db_cursor.execute("SELECT batch_size, sleep_time FROM schedules WHERE process_id = ?", (pid,))
                cron_schedule_data = cron_db_cursor.fetchone()
                global batch_size,sleep_time
                batch_size = cron_schedule_data['batch_size']
                sleep_time = cron_schedule_data['sleep_time']

                rows = src_cursor.fetchmany(batch_size)

                if not rows:
                    break
                # Setup dynamic SQL placeholders once
                if total_moved == 0:
                    placeholders = ', '.join(['%s'] * len(rows[0]))
                    insert_sql = f"REPLACE INTO {table_name} VALUES ({placeholders})"

                dest_conn.start_transaction()
                dest_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 0;")
                dest_cursor.executemany(insert_sql, rows)                  
                dest_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 1;")

                #deleting the moved rows
                if delete_source_rows and id_index is not None and (table_name in archive_tables or table_name in order_dependent_archive_table):
                    delete_status = True
                    moved_ids= [row[id_index] for row in rows]
                    delete_placeholder = ", ".join(['%s']* len(moved_ids))
                    delete_query = f"DELETE FROM {table_name} WHERE id IN ({delete_placeholder})"                      
                    src_delete_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 0;")
                    src_delete_cursor.execute(delete_query,tuple(moved_ids))
                    deleted_rows = src_delete_cursor.rowcount
                    total_deleted += deleted_rows
                    src_delete_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 1;")
                    transfer_log.info(f"Progress: {total_deleted}/{total_rows} deleted from source table {table_name}")
                    
                dest_conn.commit()    
                src_conn.commit() 
                total_moved += len(rows)
                transfer_log.info(f"Progress: {total_moved}/{total_rows} moved from table {table_name}")
                dest_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                final_dest_count = dest_cursor.fetchone()[0]
                cron_db_cursor.execute(
                    """
                    UPDATE process
                    SET scheduler_id = s.id,
                        table_name = ?,
                        converted_rows = ?,
                        dest_total_rows = ?,
                        status = ?
                    FROM schedules s
                    WHERE process.table_name = ? 
                    AND s.process_id = ?
                    """, 
                    (table_name, total_moved, final_dest_count, 'processing', table_name, pid)
                )

                dest_conn.commit()
                cron_db_conn.commit()
                sleep()
        except mysql.connector.Error as e:
            dest_conn.rollback()
            dest_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 1;")
            dest_conn.commit()
            src_delete_cursor.execute(f"SET FOREIGN_KEY_CHECKS = 1;")
            src_conn.commit()
            transfer_log.error(f"Error moving batch near row {total_moved}: {e}")
            cron_db_cursor.execute(
                """
                UPDATE process
                SET scheduler_id = s.id,
                    table_name = ?,
                    status = ?,
                    error_msg = ?
                FROM schedules s
                WHERE process.table_name = ? 
                AND process.scheduler_id = s.id
                AND s.process_id = ?
                """, 
                (table_name, 'error1', str(e), table_name, pid)
            )

            cron_db_conn.commit()

        # 4. Post-transfer Verification
        dest_cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_dest_count = dest_cursor.fetchone()[0]
        
        if (total_moved == total_deleted and total_moved == total_rows and delete_status) or (not delete_status and total_moved == total_rows):
            report = f"COMPLETE: Moved {total_moved} rows. Destination now total: {final_dest_count}"
            transfer_log.info(report)
            cron_db_cursor.execute(
                """
                UPDATE process
                SET scheduler_id = s.id,
                    table_name = ?,
                    converted_rows = ?,
                    dest_total_rows = ?,
                    status = ?,
                    error_msg = ?
                FROM schedules s
                WHERE process.table_name = ? 
                AND process.scheduler_id = s.id
                AND s.process_id = ?
                """, 
                (table_name, total_moved, final_dest_count, 'completed', '', table_name, pid)
            )

            cron_db_conn.commit()

        else:
            transfer_log.error(f"ERROR: Moved {total_moved} rows. Deleted {total_deleted} rows. when target rows {total_rows} and destination rows {final_dest_count}")
            cron_db_cursor.execute(
                """
                UPDATE process
                SET scheduler_id = (SELECT id FROM schedules WHERE process_id = ?),
                    table_name = ?,
                    converted_rows = ?,
                    dest_total_rows = ?,
                    status = ?,
                    error_msg = ?
                WHERE table_name = ? 
                AND status != 'error1'
                AND scheduler_id = (SELECT id FROM schedules WHERE process_id = ?)
                """, 
                (pid, table_name, total_moved, final_dest_count, 'error2', 'some data not deleted or moved', table_name, pid)
            )

            cron_db_conn.commit()

    except mysql.connector.Error as err:
        query_upsert = """
        INSERT INTO process (scheduler_id, table_name, status, error_msg)
        SELECT s.id, ?, ?, ?
        FROM schedules s
        WHERE s.process_id = ?
        LIMIT 1
        ON DUPLICATE KEY UPDATE
            status = VALUES(status),
            error_msg = VALUES(error_msg),
            table_name = VALUES(table_name)
        """
        cron_db_cursor.execute(query_upsert, (table_name, 'error3', str(err), pid))
        cron_db_conn.commit()
        transfer_log.critical(f"Connection failed: {err}")
    finally:
        disconnect_db(src_conn)
        disconnect_db(dest_conn)
        disconnect_db(cron_db_conn)
          
        
def get_all_tables():
    src_conn = None
    dest_conn = None
    try:
        src_conn = connect_db(source_config)
        dest_conn = connect_db(dest_config) 
        src_cursor = src_conn.cursor(buffered=True)
        src_cursor.execute(f"SHOW TABLES;")
        rows = src_cursor.fetchall()
        tables_name = [r[0] for r in rows]
        tables_name.remove('job_batches')
        return tables_name
        
    except mysql.connector.Error as err:
        transfer_log.critical(f"Connection failed: {err}")
    finally:
        disconnect_db(src_conn)
        disconnect_db(dest_conn)        
        
        
def transfer_data(target_tables):
    delete_order_table = True
    pid = os.getpid()
    global delete_source_rows
    tables_name = []
    try:
        cron_db_conn = connect_db(cron_db_config)
        cron_db_conn.row_factory = sqlite3.Row
        cron_cursor = cron_db_conn.cursor()
        cron_cursor.execute("SELECT p.table_name FROM process as p JOIN schedules as s ON p.scheduler_id=s.id WHERE s.process_id=? AND p.status = 'completed'",(pid,))
        completed_table_rows = cron_cursor.fetchall()
        completed_tables = [row['table_name'] for row in completed_table_rows]

        if not target_tables:
            tables_name = get_all_tables()
        else :
            tables_name = target_tables
        if 'job_batches' in tables_name:
            tables_name.remove('job_batches')

        transfer_log.info(f"Data transfer started {datetime.now()}")
        
        if 'orders' in tables_name:
            tables_name.remove('orders')
            delete_source_rows_temp = delete_source_rows
            try:
                delete_source_rows = False
                if 'orders' not in completed_tables:
                    transfer_table_data('orders')
            finally:
                delete_source_rows = delete_source_rows_temp
            tables_name.append('orders')
        for t_name in tables_name:
            if t_name not in completed_tables:
                transfer_table_data(t_name) 
        cron_cursor.execute("SELECT p.table_name FROM process as p JOIN schedules as s ON p.scheduler_id=s.id WHERE s.process_id=? AND p.status = 'error1' or p.status = 'error2' or p.status = 'error3'",(pid,))
        error_table_rows = cron_cursor.fetchall()
        error_tables = [row['table_name'] for row in error_table_rows]
        for t_name in error_tables:
            if t_name in order_dependent_archive_table:
                delete_order_table = False
                break
        
        if 'orders' in tables_name and delete_source_rows and delete_order_table:
            transfer_table_data('orders')
        
        transfer_log.info("data transfer completed")
        cron_cursor.execute("UPDATE schedules SET status = 'completed' WHERE process_id = ?", (pid,))
        cron_db_conn.commit()
        
    except mysql.connector.Error as err:
        transfer_log.critical(f"Connection failed: {err}")


 

def start_process(job):
    args = job

    source_config['host'] = args['src_host']
    source_config['port'] = int(args['src_port']) if args['src_port'] else 3306
    source_config['user'] = args['src_user_name']
    source_config['password'] = args['src_pass']
    source_config['database'] = args['src_db_name']
    source_config['ssl_ca'] = args['src_ca_cert']

    dest_config['host'] = args['dest_host']
    dest_config['port'] = int(args['dest_port']) if args['dest_port'] else 3306
    dest_config['user'] = args['dest_user_name']
    dest_config['password'] = args['dest_pass']
    dest_config['database'] = args['dest_db_name']
    dest_config['ssl_ca'] = args['dest_ca_cert']

        
    global target_shops
    global start_date
    global end_date
    global delete_source_rows 
    global delete_dest_rows 
    global batch_size
    global sleep_time

    batch_size = args['batch_size']
    sleep_time = args['sleep_time']
    test_connection = args['test_connection']
    start_date = args['start_date']
    end_date = args['end_date']
        
    delete_source_rows = args['delete_source_rows']
    delete_dest_rows = args['delete_source_rows']
    

    transfer_log.info(f"source_config: {source_config}")
    transfer_log.info(f"dest_config: {dest_config}")
    transfer_log.info(f"start_date: {start_date}")
    transfer_log.info(f"end_date: {end_date}")
    transfer_log.info(f"delete_source_rows: {delete_source_rows}")
    transfer_log.info(f"delete_dest_rows: {delete_dest_rows}")
    transfer_log.info(f"batch_size: {batch_size}")
    transfer_log.info(f"sleep_time: {sleep_time}")


    src= connect_db(source_config)
    dst=connect_db(dest_config)      
    if src and dst:
        transfer_log.info("database connected")
        print("database connected")
        if test_connection:
            print("test connection successful")
            transfer_log.info("test connection successful")
            return
        else :
            transfer_data(get_all_tables())

    if not src:
        transfer_log.error("source could not be connect")
        print("source could not be connect")  
    if not dst:
        transfer_log.error("dest could not be connect")
        print("dest could not be connect")

    

def main():
    global transfer_log, transfer_log_handler, transfer_log_formatter
    try:
        cron_db = connect_db(cron_db_config)   
        if cron_db:
            cron_db.row_factory = sqlite3.Row
            cron_cursor = cron_db.cursor()
            cron_cursor.execute(
                    "SELECT id, process_id, src_host, src_port, src_db_name, src_user_name, "
                    "src_pass, src_ca_cert, dest_host, dest_port, dest_db_name, dest_user_name, "
                    "dest_pass, dest_ca_cert, start_time, batch_size, sleep_time, status, "
                    "created_at, updated_at, start_date, end_date, delete_source_rows, test_connection "
                    "FROM schedules WHERE (status <> 'completed' OR status IS NULL) AND start_time <= datetime('now', 'localtime') ORDER BY start_time ASC LIMIT 1"
                )

            scheduled_job = cron_cursor.fetchone()

            if scheduled_job:
                if not scheduled_job['process_id']:
                    pid = os.getpid()
                    print("pid",pid)
                    cron_cursor.execute("UPDATE schedules SET process_id = ?, status = 'running' WHERE id = ?",(pid, scheduled_job['id']))

                    cron_db.commit()
                    start_process(scheduled_job) 
                    cron_cursor.execute("UPDATE schedules SET status = 'completed' WHERE process_id = ?", (pid,))
                    cron_db.commit() 
                else :
                    try:
                        os.kill(scheduled_job['process_id'], 0) 
                        print("Job is already running")
                        cron_log.info("Job is already running")
                        return 
                    except OSError:
                        cron_log.warning("Previous job was terminated without completion , new job will start now.")
                        print("Previous job was terminated without completion , new job will start now.")
                        
                        transfer_log.removeHandler(transfer_log_handler)
                        transfer_log_handler = logging.FileHandler('transfer_log.log',mode='a')
                        transfer_log_handler.setFormatter(transfer_log_formatter)
                        transfer_log.addHandler(transfer_log_handler)

                        pid = os.getpid()
                        cron_cursor.execute("UPDATE schedules SET process_id = ?, status = 'running' WHERE id = ?",(pid, scheduled_job['id']))
                        cron_db.commit()
                        start_process(scheduled_job) 
                        cron_cursor.execute("UPDATE schedules SET status = 'completed' WHERE process_id = ?", (pid,))
                        cron_db.commit() 
            else:
                print("No scheduled job found")
                cron_log.info("No scheduled job found to execute")
            
    except Exception as e:
        cron_log.error(f"Error: {e}")
        print(f"Error: {e}")
    finally:
        if cron_db:
            disconnect_db(cron_db)


if __name__ == '__main__':
    main()
    print("Process finished")
    cron_log.info("Process finished")
    
