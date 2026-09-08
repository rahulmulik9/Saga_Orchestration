-- Seed data for payment-service (Phase 2)
-- Runs on every startup since ddl-auto=create wipes and recreates the schema.
-- Save this file as: src/main/resources/data.sql

-- ===== PAYMENT =====
-- These are standalone historical rows for testing GET /payments/{id};
-- they do NOT correspond to real orders in order-service's DB (each
-- service owns its own data now, so there's no live FK relationship).
INSERT INTO payment (id, order_id, amount, status, created_at) VALUES (1, 1, 19.98, 'SUCCESS', now());
INSERT INTO payment (id, order_id, amount, status, created_at) VALUES (2, 2, 12000.00, 'FAILED', now());
INSERT INTO payment (id, order_id, amount, status, created_at) VALUES (3, 3, 19.99, 'PENDING', now());
INSERT INTO payment (id, order_id, amount, status, created_at) VALUES (4, 4, 73.50, 'SUCCESS', now());

-- Ensure Postgres identity sequence doesn't collide with these explicit ids
-- on subsequent inserts made through the app after startup.
SELECT setval(pg_get_serial_sequence('payment', 'id'), (SELECT MAX(id) FROM payment));
