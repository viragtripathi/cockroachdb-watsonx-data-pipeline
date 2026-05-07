-- Manual CDC changes for the live demo.
-- ========================================
-- Run these against your local CockroachDB during the demo to generate
-- update/delete events that flow through the pipeline into Iceberg in
-- real time. After ~15-30 seconds you'll be able to:
--   - Show all three cdc_operation values (insert / update / delete)
--   - Run the hybrid JOIN with non-zero price deltas
--   - Run the ghost-records query and see the deleted rows preserved in CDC
--
-- Usage from the host:
--   cockroach sql --insecure \
--     --url "postgresql://root@localhost:26257/defaultdb?sslmode=disable" \
--     < sql/demo-cdc-changes.sql
--
-- Or paste blocks individually for theatrical pacing.

-- ============================================================
-- BLOCK 1: 5 price corrections  ->  cdc_operation = 'update'
-- ============================================================
-- Bumps the 5 highest-amount expenses by 15% and tags the description.
-- Lands as 5 update events in Iceberg.

UPDATE expenses
SET expense_amount = expense_amount * 1.15,
    description = description || ' (price correction for IBM Think demo)'
WHERE expense_id IN (
  SELECT expense_id FROM expenses
  ORDER BY expense_amount DESC
  LIMIT 5
);

-- ============================================================
-- BLOCK 2: 3 deletions  ->  cdc_operation = 'delete'  (ghost records)
-- ============================================================
-- Removes the 3 smallest expenses from CRDB. They live on in Iceberg
-- with cdc_operation='delete' -- the "ghost record" pattern.

DELETE FROM expenses
WHERE expense_id IN (
  SELECT expense_id FROM expenses
  ORDER BY expense_amount ASC
  LIMIT 3
);

-- ============================================================
-- BLOCK 3 (optional): a single fresh insert with a memorable merchant
-- ============================================================
-- Useful if you want to show "live insert hits Iceberg in real time"
-- during the audience Q&A. The merchant name makes it easy to find.

INSERT INTO expenses (expense_id, user_id, expense_date, expense_amount,
                      shopping_type, description, merchant, payment_method, recurring)
VALUES (
    gen_random_uuid(),
    (SELECT user_id FROM expenses LIMIT 1),
    current_date,
    99.99,
    'Travel',
    'Demo INSERT triggered live at IBM Think',
    'IBM Think Demo Merchant',
    'Credit Card',
    false
);

-- ============================================================
-- Verify locally before running the watsonx.data queries
-- ============================================================
-- SELECT expense_id, expense_amount, description
-- FROM expenses
-- WHERE description LIKE '%IBM Think%'
-- ORDER BY expense_amount DESC;
