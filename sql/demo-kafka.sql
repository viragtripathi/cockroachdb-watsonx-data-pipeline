-- Demo data for the Kafka/Debezium CDC pipeline.
-- Run against the local Docker CockroachDB:
--   docker exec -i crdb-source cockroach sql --insecure < sql/demo-kafka.sql
--
-- Generates ~1000 CDC events: 800 inserts, 150 updates, 50 deletes.

-- ============================================================
-- PHASE 1: INSERTS (800 rows)
-- ============================================================

-- Coffee (100 rows)
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
SELECT gen_random_uuid(),
       'Morning coffee #' || g,
       (ARRAY['Starbucks','Dunkin','Peets Coffee','Blue Bottle','Tim Hortons'])[1 + (g % 5)],
       round((3 + (g % 8))::numeric + (g % 100)::numeric / 100, 2),
       current_date() - (g % 90),
       'Coffee',
       (ARRAY['Debit Card','Credit Card','Apple Pay','Google Pay'])[1 + (g % 4)],
       (g % 3 = 0)
FROM generate_series(1, 100) AS g;

-- Groceries (100 rows)
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
SELECT gen_random_uuid(),
       'Groceries run #' || g,
       (ARRAY['Whole Foods','Trader Joes','Costco','Safeway','Kroger'])[1 + (g % 5)],
       round((20 + (g * 1.8))::numeric, 2),
       current_date() - (g % 90),
       'Groceries',
       (ARRAY['Debit Card','Credit Card','Apple Pay','Google Pay'])[1 + (g % 4)],
       (g % 2 = 0)
FROM generate_series(1, 100) AS g;

-- Transport (100 rows)
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
SELECT gen_random_uuid(),
       'Ride #' || g,
       (ARRAY['Uber','Lyft','Shell','BP','Chevron'])[1 + (g % 5)],
       round((5 + (g % 55))::numeric + (g % 100)::numeric / 100, 2),
       current_date() - (g % 90),
       'Transport',
       (ARRAY['Debit Card','Credit Card','Apple Pay','Google Pay'])[1 + (g % 4)],
       (g % 5 = 0)
FROM generate_series(1, 100) AS g;

-- Restaurant (100 rows)
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
SELECT gen_random_uuid(),
       'Dinner #' || g,
       (ARRAY['Italian Bistro','Chipotle','Olive Garden','Sushi Place','Thai Kitchen'])[1 + (g % 5)],
       round((15 + (g * 1.1))::numeric, 2),
       current_date() - (g % 90),
       'Restaurant',
       (ARRAY['Credit Card','Debit Card','Apple Pay','Google Pay'])[1 + (g % 4)],
       false
FROM generate_series(1, 100) AS g;

-- Entertainment (80 rows)
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
SELECT gen_random_uuid(),
       'Entertainment #' || g,
       (ARRAY['Netflix','Spotify','Disney+','HBO Max','Apple TV'])[1 + (g % 5)],
       round((5 + (g % 50))::numeric + 0.99, 2),
       current_date() - (g % 90),
       'Entertainment',
       (ARRAY['Credit Card','Debit Card'])[1 + (g % 2)],
       (g % 2 = 0)
FROM generate_series(1, 80) AS g;

-- Shopping (80 rows)
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
SELECT gen_random_uuid(),
       'Shopping #' || g,
       (ARRAY['Amazon','Target','Walmart','Best Buy','Macys'])[1 + (g % 5)],
       round((10 + (g * 3.7))::numeric, 2),
       current_date() - (g % 90),
       'Shopping',
       (ARRAY['Credit Card','Debit Card','Apple Pay'])[1 + (g % 3)],
       false
FROM generate_series(1, 80) AS g;

-- Health (80 rows)
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
SELECT gen_random_uuid(),
       'Health #' || g,
       (ARRAY['Planet Fitness','CVS Pharmacy','Walgreens','GNC','Peloton'])[1 + (g % 5)],
       round((10 + (g % 80))::numeric + 0.50, 2),
       current_date() - (g % 90),
       'Health',
       (ARRAY['Debit Card','Credit Card'])[1 + (g % 2)],
       (g % 3 = 0)
FROM generate_series(1, 80) AS g;

-- Electronics (60 rows)
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
SELECT gen_random_uuid(),
       'Electronics #' || g,
       (ARRAY['Best Buy','Apple Store','Micro Center','Newegg','B&H Photo'])[1 + (g % 5)],
       round((20 + (g * 8))::numeric, 2),
       current_date() - (g % 90),
       'Electronics',
       (ARRAY['Credit Card','Apple Pay'])[1 + (g % 2)],
       false
FROM generate_series(1, 60) AS g;

-- ============================================================
-- PHASE 2: UPDATES (150 rows)
-- ============================================================

-- Update Coffee amounts (25 rows)
UPDATE expenses
SET expense_amount = expense_amount + 2.50,
    description = description || ' (size upgrade)'
WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Coffee' ORDER BY created_at DESC LIMIT 25
);

-- Update Groceries payment method (25 rows)
UPDATE expenses
SET payment_method = 'Apple Pay',
    description = description || ' (updated payment)'
WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Groceries' ORDER BY created_at DESC LIMIT 25
);

-- Update Transport amounts (25 rows)
UPDATE expenses
SET expense_amount = expense_amount + 5.00,
    description = description || ' (tip added)'
WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Transport' ORDER BY created_at DESC LIMIT 25
);

-- Update Restaurant split bills (25 rows)
UPDATE expenses
SET expense_amount = round((expense_amount / 2)::numeric, 2),
    description = description || ' (split bill)'
WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Restaurant' ORDER BY created_at DESC LIMIT 25
);

-- Update Shopping to Prime (25 rows)
UPDATE expenses
SET merchant = 'Amazon Prime',
    description = description || ' (Prime order)'
WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Shopping' ORDER BY created_at DESC LIMIT 25
);

-- Update Entertainment to recurring (25 rows)
UPDATE expenses
SET recurring = true,
    description = description || ' (now recurring)'
WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Entertainment' AND recurring = false ORDER BY created_at DESC LIMIT 25
);

-- ============================================================
-- PHASE 3: DELETES (50 rows)
-- ============================================================

DELETE FROM expenses WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Coffee' ORDER BY created_at LIMIT 10
);

DELETE FROM expenses WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Transport' ORDER BY created_at LIMIT 10
);

DELETE FROM expenses WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Entertainment' ORDER BY created_at LIMIT 10
);

DELETE FROM expenses WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Shopping' ORDER BY created_at LIMIT 10
);

DELETE FROM expenses WHERE expense_id IN (
  SELECT expense_id FROM expenses WHERE shopping_type = 'Health' ORDER BY created_at LIMIT 10
);

-- ============================================================
-- SUMMARY
-- ============================================================
SELECT 'Demo complete' AS status,
       count(*) AS remaining_rows,
       count(DISTINCT shopping_type) AS categories,
       count(DISTINCT merchant) AS merchants
FROM expenses;
