SET FOREIGN_KEY_CHECKS = 0;

DELETE FROM activity_log WHERE created_at < '2025-07-01 00:00:00';
DELETE FROM fake_order_settings WHERE created_at < '2025-07-01 00:00:00';

DELETE FROM sales WHERE created_at < '2025-07-01 00:00:00';
DELETE FROM sales_target_histories WHERE created_at < '2025-07-01 00:00:00';
DELETE FROM sales_targets WHERE created_at < '2025-07-01 00:00:00';
DELETE FROM call_histories WHERE created_at < '2025-07-01 00:00:00';

DELETE comments FROM comments 
INNER JOIN orders ON comments.order_id = orders.id 
WHERE orders.created_at < '2025-07-01 00:00:00';

DELETE applied_coupons FROM applied_coupons 
INNER JOIN orders ON applied_coupons.order_id = orders.id 
WHERE orders.created_at < '2025-07-01 00:00:00';

DELETE agent_assign_logs FROM agent_assign_logs 
INNER JOIN orders ON agent_assign_logs.order_id = orders.id 
WHERE orders.created_at < '2025-07-01 00:00:00';

DELETE call_initiation_logs FROM call_initiation_logs 
INNER JOIN orders ON call_initiation_logs.order_id = orders.id 
WHERE orders.created_at < '2025-07-01 00:00:00';

DELETE call_automation_logs FROM call_automation_logs 
INNER JOIN orders ON call_automation_logs.order_id = orders.id 
WHERE orders.created_at < '2025-07-01 00:00:00';

DELETE order_items FROM order_items 
INNER JOIN orders ON order_items.order_id = orders.id 
WHERE orders.created_at < '2025-07-01 00:00:00';

DELETE order_logs FROM order_logs 
INNER JOIN orders ON order_logs.order_id = orders.id 
WHERE orders.created_at < '2025-07-01 00:00:00';

DELETE order_metas FROM order_metas 
INNER JOIN orders ON order_metas.order_id = orders.id 
WHERE orders.created_at < '2025-07-01 00:00:00';


DELETE FROM orders WHERE created_at < '2025-07-01 00:00:00';

SET FOREIGN_KEY_CHECKS = 1;