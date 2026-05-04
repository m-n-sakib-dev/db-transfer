SELECT 'activity_log' as table_name, COUNT(*) as row_count FROM activity_log
UNION ALL SELECT 'fake_order_settings', COUNT(*) FROM fake_order_settings
UNION ALL SELECT 'sales', COUNT(*) FROM sales
UNION ALL SELECT 'sales_target_histories', COUNT(*) FROM sales_target_histories
UNION ALL SELECT 'sales_targets', COUNT(*) FROM sales_targets
UNION ALL SELECT 'call_histories', COUNT(*) FROM call_histories
UNION ALL SELECT 'comments', COUNT(*) FROM comments
UNION ALL SELECT 'applied_coupons', COUNT(*) FROM applied_coupons
UNION ALL SELECT 'agent_assign_logs', COUNT(*) FROM agent_assign_logs
UNION ALL SELECT 'call_initiation_logs', COUNT(*) FROM call_initiation_logs
UNION ALL SELECT 'call_automation_logs', COUNT(*) FROM call_automation_logs
UNION ALL SELECT 'order_items', COUNT(*) FROM order_items
UNION ALL SELECT 'order_logs', COUNT(*) FROM order_logs
UNION ALL SELECT 'order_metas', COUNT(*) FROM order_metas
UNION ALL SELECT 'orders', COUNT(*) FROM orders;
