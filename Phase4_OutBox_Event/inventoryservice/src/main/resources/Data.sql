-- Seed data for inventory-service (Phase 2)
-- Runs on every startup since ddl-auto=create wipes and recreates the schema.
-- Save this file as: src/main/resources/data.sql

-- ===== PRODUCT =====
-- id=1: enough stock for happy-path orders, low price
INSERT INTO product (id, name, price, quantity) VALUES (1, 'Widget', 9.99, 10);
-- id=2: priced so ordering 2+ units crosses the payment failure threshold (>= 10000)
INSERT INTO product (id, name, price, quantity) VALUES (2, 'Premium Gadget', 6000.00, 5);
-- id=3: low stock, useful for triggering insufficient-stock (400) path
INSERT INTO product (id, name, price, quantity) VALUES (3, 'Rare Item', 19.99, 1);
-- id=4: general extra product
INSERT INTO product (id, name, price, quantity) VALUES (4, 'Gizmo', 24.50, 20);

-- Ensure Postgres identity sequence doesn't collide with these explicit ids
-- on subsequent inserts made through the app after startup.
SELECT setval(pg_get_serial_sequence('product', 'id'), (SELECT MAX(id) FROM product));
