from mysql.connector import optionfiles
import mysql.connector
import logging
from datetime import datetime
import argparse
import sys


target_shops = []
batch_size = 10000  # Number of rows to move per batch
sleept_time = 1
start_date = '2025-07-01 00:00:00'
end_date = '2026-07-01 00:00:00'
delete_source_rows = False
delete_dest_rows = False
source_config = {
    'host': '',
    'port': '',     
    'user': '',
    'password': '',
    'database' : '',
    'ssl_ca': "",

}

dest_config = {
    'host': '127.0.0.1',
    'port': 3306,     
    'user': 'root',
    'password': '',
    'database': 'backupdatabase',
    'ssl_ca': ""
}

logging.basicConfig(
    filename='test_log.log',
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
        return [None,None]
        
        
def main():
    src,dst=connect_db()      
    if src and dst:
        logging.info("database connected")
    if not src:
        logging.error("source could not be connect")  
    if not dst:
        logging.error("dest could not be connect")
        
        
def get_args():
    parser = argparse.ArgumentParser(description="Database Archiving Script")

    # Mandatory Arguments
    parser.add_argument("--src_host", required=True, help="Source Host IP")
    parser.add_argument("--src_port", required=True, help="Source Host port")
    parser.add_argument("--src_user", required=True, help="Source Host User")
    parser.add_argument("--src_password", required=True, help="Source Host Password")
    parser.add_argument("--src_db", required=True, help="Source Database name")
    parser.add_argument("--src_ssl_ca", default="", help="Source Database SSL needed")
    
    parser.add_argument("--destination_host", required=True, help="Destination Host IP")
    parser.add_argument("--destination_port", required=True, help="Destination Host port")
    parser.add_argument("--destination_user", required=True, help="Destination Host User")
    parser.add_argument("--destination_password", required=True, help="Destination Host Password")
    parser.add_argument("--destination_db", required=True, help="Destination Database name")
    parser.add_argument("--destination_ssl_ca", default="", help="Dest Database SSL needed")
    
    parser.add_argument("--start_date", required=True, help="Start Date (YYYY-MM-DD)")
    parser.add_argument("--end_date", required=True, help="Date Date (YYYY-MM-DD)")
    
    parser.add_argument("--batch_size", type=int, default=10000, help="Number of rows to move per batch")
    parser.add_argument("--sleep_time", type=int, default=1, help="Sleep time between batches")

    # Optional Arguments (Defaults to empty list/None)
    parser.add_argument("--tables", nargs='*', default=[], help="Specific tables (space separated). Leave empty for all.")
    parser.add_argument("--shop_ids", nargs='*', default=[], help="Specific Shop IDs (space separated). Leave empty for all.")
    parser.add_argument("--delete_source_rows", action="store_true", help="Mention to delete source table rows that moved to destination")
    parser.add_argument("--delete_dest_rows", action="store_true", help="Mention to delete destination tables old data.")
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
    source_config['ssl_ca'] = args.src_ssl_ca
    

    # Updating global dest_config dictionary
    dest_config['host'] = args.destination_host
    dest_config['port'] = int(args.destination_port) if args.destination_port else 3306
    dest_config['user'] = args.destination_user
    dest_config['password'] = args.destination_password
    dest_config['database'] = args.destination_db
    dest_config['ssl_ca'] = args.destination_ssl_ca


        
    global target_shops
    global start_date
    global end_date
    global delete_source_rows 
    global delete_dest_rows 
    global batch_size
    global sleept_time

    batch_size = args.batch_size
    sleept_time = args.sleep_time
    tables_to_move = args.tables
    target_shops = args.shop_ids
    
    # Date Validation
    try:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
    except ValueError:
        logging.error("Error: Dates must be in YYYY-MM-DD format.")
        sys.exit(1)
        
    delete_source_rows = args.delete_source_rows
    delete_dest_rows = args.delete_source_rows
    

    print(f"source_config: {source_config}")
    print(f"dest_config: {dest_config}")
    print(f"tables_to_move: {tables_to_move}")
    print(f"shops_to_move: {target_shops }")
    print(f"start_date: {start_date}")
    print(f"end_date: {end_date}")
    print(f"delete_source_rows: {delete_source_rows}")
    print(f"delete_dest_rows: {delete_dest_rows}")
    print(f"batch_size: {batch_size}")
    print(f"sleept_time: {sleept_time}")
    
    src,dst=connect_db()      
    if src and dst:
        logging.info("database connected")
        print("database connected")
    if not src:
        logging.error("source could not be connect")
        print("source could not be connect")  
    if not dst:
        logging.error("dest could not be connect")
        print("dest could not be connect")



if __name__ == '__main__':
    main()      