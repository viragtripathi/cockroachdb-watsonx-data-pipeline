-- CockroachDB setup for the CDC pipeline demo.
-- This creates the expenses table matching the Banko AI schema.

SET CLUSTER SETTING kv.rangefeed.enabled = true;

CREATE DATABASE IF NOT EXISTS defaultdb;
USE defaultdb;

CREATE TABLE IF NOT EXISTS expenses (
    expense_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    description STRING,
    merchant STRING NOT NULL,
    expense_amount DECIMAL(12,2) NOT NULL,
    expense_date DATE NOT NULL DEFAULT current_date(),
    shopping_type STRING DEFAULT 'General',
    payment_method STRING DEFAULT 'Credit Card',
    recurring BOOLEAN DEFAULT false,
    tags STRING[],
    embedding VECTOR(384),
    created_at TIMESTAMPTZ DEFAULT current_timestamp()
);

-- Sample data
INSERT INTO expenses (user_id, description, merchant, expense_amount, expense_date, shopping_type, payment_method, recurring)
VALUES
    (gen_random_uuid(), 'Coffee at Starbucks', 'Starbucks', 5.50, '2025-03-01', 'Coffee', 'Debit Card', true),
    (gen_random_uuid(), 'Groceries at Whole Foods', 'Whole Foods', 87.23, '2025-03-02', 'Groceries', 'Credit Card', true),
    (gen_random_uuid(), 'Electronics at Best Buy', 'Best Buy', 299.99, '2025-03-03', 'Electronics', 'Credit Card', false),
    (gen_random_uuid(), 'Gas at Shell', 'Shell', 45.00, '2025-03-04', 'Transport', 'Debit Card', true),
    (gen_random_uuid(), 'Restaurant at Italian Bistro', 'Italian Bistro', 104.29, '2025-03-05', 'Restaurant', 'Apple Pay', false);

-- Grant changefeed permissions
GRANT CHANGEFEED ON TABLE expenses TO root;
