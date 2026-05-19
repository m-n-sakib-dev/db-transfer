import sqlite3
import os

def setup_database():
    # 1. Connect to SQLite database (Creates the file if it does not exist)
    folder_name = "database"
    db_name = os.path.join(folder_name, "cron_db.db")
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    print(f"Connected to database: {db_name}")

    try:
        # 2. MANDATORY: Enable Foreign Key support in SQLite
        cursor.execute("PRAGMA foreign_keys = ON;")

        # 3. Create 'schedules' table (Must be first)
        print("Creating 'schedules' table...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS schedules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            process_id INTEGER DEFAULT NULL,
            src_host TEXT NOT NULL,
            src_port INTEGER NOT NULL,
            src_db_name TEXT NOT NULL,
            src_user_name TEXT NOT NULL,
            src_pass TEXT NOT NULL,
            src_ca_cert TEXT,
            dest_host TEXT NOT NULL,
            dest_port INTEGER NOT NULL,
            dest_db_name TEXT NOT NULL,
            dest_user_name TEXT NOT NULL,
            dest_pass TEXT NOT NULL,
            dest_ca_cert TEXT,
            start_time TEXT DEFAULT NULL,
            batch_size INTEGER DEFAULT 1000,
            sleep_time INTEGER DEFAULT 0,
            status TEXT DEFAULT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            start_date TEXT NOT NULL,
            end_date TEXT NOT NULL,
            delete_source_rows INTEGER DEFAULT 0,
            test_connection INTEGER DEFAULT 0
        );
        ''')

        # 4. Create 'process' table (Contains Foreign Key linking to schedules)
        print("Creating 'process' table...")
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS process (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scheduler_id INTEGER NOT NULL,
            table_name TEXT NOT NULL,
            target_total_rows INTEGER DEFAULT 0,
            converted_rows INTEGER DEFAULT 0,
            dest_total_rows INTEGER DEFAULT 0,
            status TEXT DEFAULT 'pending',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            error_msg TEXT,
            UNIQUE (scheduler_id, table_name),
            CONSTRAINT fk_schedules FOREIGN KEY (scheduler_id) REFERENCES schedules (id) ON DELETE CASCADE
        );
        ''')

        # 5. Create Auto-Timestamp Triggers for updates
        print("Setting up timestamp triggers...")
        cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS ts_update_schedules AFTER UPDATE ON schedules
        BEGIN
            UPDATE schedules SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;
        ''')

        cursor.execute('''
        CREATE TRIGGER IF NOT EXISTS ts_update_process AFTER UPDATE ON process
        BEGIN
            UPDATE process SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
        END;
        ''')

        # 6. Commit structural changes
        conn.commit()
        print("Database structure initialized successfully!")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
        
    finally:
        # 7. Always close the connection
        conn.close()

if __name__ == "__main__":
    setup_database()
