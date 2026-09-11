-- Seed data for order-service (Phase 2)
-- Runs on every startup since ddl-auto=create wipes and recreates the schema.
-- Save this file as: src/main/resources/data.sql

-- ===== ORDERS =====
INSERT INTO orders (id, status, created_at) VALUES (1, 'COMPLETED', now());
INSERT INTO orders (id, status, created_at) VALUES (2, 'FAILED', now());
INSERT INTO orders (id, status, created_at) VALUES (3, 'PENDING', now());
INSERT INTO orders (id, status, created_at) VALUES (4, 'COMPLETED', now());

-- ===== ORDER_ITEM =====
-- product_id here is just a stored reference value now (no live FK to
-- inventory-service's DB - each service owns its own schema).
INSERT INTO order_item (id, product_id, quantity, price, order_id) VALUES (1, 1, 2, 9.99, 1);
INSERT INTO order_item (id, product_id, quantity, price, order_id) VALUES (2, 2, 2, 6000.00, 2);
INSERT INTO order_item (id, product_id, quantity, price, order_id) VALUES (3, 3, 1, 19.99, 3);
INSERT INTO order_item (id, product_id, quantity, price, order_id) VALUES (4, 4, 3, 24.50, 4);

-- Ensure Postgres identity sequences don't collide with these explicit ids
-- on subsequent inserts made through the app after startup.
SELECT setval(pg_get_serial_sequence('orders', 'id'), (SELECT MAX(id) FROM orders));
SELECT setval(pg_get_serial_sequence('order_item', 'id'), (SELECT MAX(id) FROM order_item));
