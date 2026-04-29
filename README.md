# db-transfer

1. Test with everything (Specific Tables & Specific Shops)

python3 archive.py \
--src_host 127.0.0.1 --src_port 3306 --src_user root --src_password '' --src_db archive_db_2 \
--archive_host 127.0.0.1 --archive_port 3306 --archive_user root --archive_password '' --archive_db backupdatabase \
--start_date 2025-07-01 \
--tables orders order_items \
--shop_ids 2444 2023 1789

2. Test with ALL Tables (Empty Tables list)

python3 archive.py \
--src_host 127.0.0.1 --src_port 3306 --src_user root --src_password '' --src_db archive_db_2 \
--archive_host 127.0.0.1 --archive_port 3306 --archive_user root --archive_password '' --archive_db backupdatabase \
--start_date 2025-07-01 \
--shop_ids 2444 2023 1789

3. Test with ALL Shops (Empty Shop list)

python3 archive.py \
--src_host 127.0.0.1 --src_port 3306 --src_user root --src_password '' --src_db archive_db_2 \
--archive_host 127.0.0.1 --archive_port 3306 --archive_user root --archive_password '' --archive_db backupdatabase \
--start_date 2024-01-01 \
--tables orders


4. Test with ALL Tables & ALL Shops (The "Full Sync")

python3 archive.py \
--src_host 127.0.0.1 --src_port 3306 --src_user root --src_password '' --src_db archive_db_2 \
--archive_host 127.0.0.1 --archive_port 3306 --archive_user root --archive_password '' --archive_db backupdatabase \
--start_date 2024-01-01

