-- Seed data for order-app (Phase 1 - Monolith)
-- Runs on every startup since ddl-auto=create wipes and recreates the schema.
-- Explicit ids used so order_item/payment can reference orders predictably.

-- ===== PRODUCT =====
-- id=1: enough stock for happy-path orders, low price
INSERT INTO product (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 10);
-- id=2: priced so ordering 2+ units crosses the payment failure threshold (>= 10000)
INSERT INTO product (id, name, price, quantity) VALUES (2, 'Premium Gadget', 6000.00, 5);
-- id=3: low stock, useful for triggering insufficient-stock (400) path
INSERT INTO product (id, name, price, quantity) VALUES (3, 'Rare Item', 19.99, 1);
-- id=4: general extra product
INSERT INTO product (id, name, price, quantity) VALUES (4, 'Gizmo', 24.50, 20);

-- ===== ORDERS =====
INSERT INTO orders (id, status, created_at) VALUES (1, 'COMPLETED', now());
INSERT INTO orders (id, status, created_at) VALUES (2, 'FAILED', now());
INSERT INTO orders (id, status, created_at) VALUES (3, 'PENDING', now());
INSERT INTO orders (id, status, created_at) VALUES (4, 'COMPLETED', now());

-- ===== ORDER_ITEM =====
INSERT INTO order_item (id, product_id, quantity, price, order_id) VALUES (1, 1, 2, 9.99, 1);
INSERT INTO order_item (id, product_id, quantity, price, order_id) VALUES (2, 2, 2, 6000.00, 2);
INSERT INTO order_item (id, product_id, quantity, price, order_id) VALUES (3, 3, 1, 19.99, 3);
INSERT INTO order_item (id, product_id, quantity, price, order_id) VALUES (4, 4, 3, 24.50, 4);

-- ===== PAYMENT =====
INSERT INTO payment (id, order_id, amount, status, created_at) VALUES (1, 1, 19.98, 'SUCCESS', now());
INSERT INTO payment (id, order_id, amount, status, created_at) VALUES (2, 2, 12000.00, 'FAILED', now());
INSERT INTO payment (id, order_id, amount, status, created_at) VALUES (3, 3, 19.99, 'PENDING', now());
INSERT INTO payment (id, order_id, amount, status, created_at) VALUES (4, 4, 73.50, 'SUCCESS', now());

-- Ensure Postgres identity sequences don't collide with these explicit ids
-- on subsequent inserts made through the app after startup.
SELECT setval(pg_get_serial_sequence('product', 'id'), (SELECT MAX(id) FROM product));
SELECT setval(pg_get_serial_sequence('orders', 'id'), (SELECT MAX(id) FROM orders));
SELECT setval(pg_get_serial_sequence('order_item', 'id'), (SELECT MAX(id) FROM order_item));
SELECT setval(pg_get_serial_sequence('payment', 'id'), (SELECT MAX(id) FROM payment));