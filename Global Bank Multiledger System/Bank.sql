-- Databricks notebook source
-- ============================================================
-- ENVIRONMENT SETUP
-- ============================================================

CREATE CATALOG IF NOT EXISTS banking;
CREATE SCHEMA IF NOT EXISTS banking.bank_core;

USE CATALOG banking;
USE SCHEMA bank_core;

-- COMMAND ----------

-- ============================================================
-- CLEAN RESET (DROP TABLES)
-- ============================================================

DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS wallets;
DROP TABLE IF EXISTS fx_rates;
DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS ledger;

-- COMMAND ----------

-- ============================================================
-- TABLE VALIDATION CHECK
-- ============================================================

-- Verify table existence via Information Schema
SELECT
  t.table_name,
  CASE
    WHEN i.table_name IS NOT NULL THEN '✅ EXISTS'
    ELSE '❌ NOT FOUND'
  END AS status
FROM (
  SELECT 'users' AS table_name UNION ALL
  SELECT 'wallets' UNION ALL
  SELECT 'fx_rates' UNION ALL
  SELECT 'transactions'
) t
LEFT JOIN system.information_schema.tables i
  ON t.table_name = i.table_name
 AND i.table_schema  = current_schema()
 AND i.table_catalog = current_catalog();

-- COMMAND ----------

-- ============================================================
-- TABLE CREATION
-- ============================================================

CREATE TABLE users (
    user_id INT,
    name STRING,
    base_currency STRING,
    created_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- WALLETS balance table
CREATE TABLE wallets (
    wallet_id STRING,
    user_id INT,
    currency STRING,
    balance DECIMAL(20,2),
    updated_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- FX conversion engine
CREATE TABLE fx_rates (
    from_currency STRING,
    to_currency STRING,
    rate DECIMAL(20,2),
    updated_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- TRANSACTIONS history table
CREATE TABLE transactions (
    txn_id STRING,
    from_user INT,
    to_user INT,
    from_currency STRING,
    to_currency STRING,
    from_amount DECIMAL(20,2),
    to_amount DECIMAL(20,2),
    fx_rate DECIMAL(20,2),
    txn_timestamp TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- LEDGER accounting journal (future-proofing)
CREATE TABLE ledger (
    entry_id STRING,
    user_id INT,
    currency STRING,
    amount DECIMAL(20,2),
    entry_type STRING,
    reference_txn STRING,
    created_at TIMESTAMP
)
USING DELTA;

-- COMMAND ----------

-- ============================================================
-- SEED USERS DATA
-- ============================================================

INSERT INTO users VALUES
(1,  'Rahul',    'INR', current_timestamp()),
(2,  'John',     'USD', current_timestamp()),
(3,  'Lucas',    'CAD', current_timestamp()),
(4,  'Emma',     'GBP', current_timestamp()),
(5,  'Marie',    'EUR', current_timestamp()),
(6,  'Sofia',    'DKK', current_timestamp()),
(7,  'Chloe',    'CHF', current_timestamp()),
(8,  'Ava',      'AUD', current_timestamp()),
(9,  'Isabella', 'NZD', current_timestamp()),
(10, 'Ivan',     'RUB', current_timestamp()),
(11, 'Ahmed',    'AED', current_timestamp()),
(12, 'Fahad',    'KWD', current_timestamp());


-- COMMAND ----------

-- ============================================================
-- SEED WALLETS DATA
-- ============================================================

INSERT INTO wallets VALUES
(101, 1,  'INR', 0,  current_timestamp()),
(102, 2,  'USD', 0,  current_timestamp()),
(103, 3,  'CAD', 0,  current_timestamp()),
(104, 4,  'GBP', 0,  current_timestamp()),
(105, 5,  'EUR', 0,  current_timestamp()),
(106, 6,  'DKK', 0,  current_timestamp()),
(107, 7,  'CHF', 0,  current_timestamp()),
(108, 8,  'AUD', 0,  current_timestamp()),
(109, 9,  'NZD', 0,  current_timestamp()),
(110, 10, 'RUB', 0,  current_timestamp()),
(111, 11, 'AED', 0,  current_timestamp()),
(112, 12, 'KWD', 0,  current_timestamp());

-- COMMAND ----------

INSERT INTO transactions
SELECT uuid(), NULL, 1,  'INR', 'INR', 0, 5000, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 2,  'USD', 'USD', 0, 3200, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 3,  'CAD', 'CAD', 0, 6400, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 4,  'GBP', 'GBP', 0, 2800, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 5,  'EUR', 'EUR', 0, 7600, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 6,  'DKK', 'DKK', 0, 4300, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 7,  'CHF', 'CHF', 0, 8900, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 8,  'AUD', 'AUD', 0, 5100, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 9,  'NZD', 'NZD', 0, 3700, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 10, 'RUB', 'RUB', 0, 9200, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 11, 'AED', 'AED', 0, 6100, 1, current_timestamp()
UNION ALL
SELECT uuid(), NULL, 12, 'KWD', 'KWD', 0, 7400, 1, current_timestamp();

-- COMMAND ----------

-- ============================================================
-- SEED FX RATES
-- ============================================================

INSERT INTO fx_rates VALUES
('USD','USD',1.00,current_timestamp()),

('USD','INR',91.70,current_timestamp()),
('INR','USD',0.01,current_timestamp()),

('USD','GBP',0.76,current_timestamp()),
('GBP','USD',1.32,current_timestamp()),

('USD','EUR',0.92,current_timestamp()),
('EUR','USD',1.09,current_timestamp()),

('USD','CAD',1.34,current_timestamp()),
('CAD','USD',0.75,current_timestamp()),

('USD','AUD',1.62,current_timestamp()),
('AUD','USD',0.62,current_timestamp()),

('USD','NZD',1.55,current_timestamp()),
('NZD','USD',0.65,current_timestamp()),

('USD','DKK',6.98,current_timestamp()),
('DKK','USD',0.14,current_timestamp()),

('USD','RUB',89.00,current_timestamp()),
('RUB','USD',0.01,current_timestamp()),

('USD','AED',3.67,current_timestamp()),
('AED','USD',0.27,current_timestamp()),

('USD','KWD',0.31,current_timestamp()),
('KWD','USD',3.23,current_timestamp()),

('USD','CHF',0.89,current_timestamp()),
('CHF','USD',1.12,current_timestamp());

-- COMMAND ----------

SELECT * FROM users
ORDER BY user_id;

-- COMMAND ----------

SELECT * FROM wallets
ORDER BY user_id;

-- COMMAND ----------

SELECT * FROM fx_rates;

-- COMMAND ----------

SELECT * FROM transactions;

-- COMMAND ----------

SELECT user_id, currency, balance
FROM wallets
ORDER BY user_id;

-- COMMAND ----------

-- ============================================================
-- SAFE WALLET RECONCILIATION
-- ============================================================

WITH txn_impact AS (

    SELECT
        from_user AS user_id,
        from_currency AS currency,
        -from_amount AS amount
    FROM transactions
    WHERE from_user IS NOT NULL

    UNION ALL

    SELECT
        to_user AS user_id,
        to_currency AS currency,
        to_amount AS amount
    FROM transactions
    WHERE to_user IS NOT NULL
),

net_changes AS (

    SELECT
        user_id,
        currency,
        SUM(amount) AS net_amount
    FROM txn_impact
    GROUP BY user_id, currency
)

MERGE INTO wallets w
USING net_changes n

ON w.user_id = n.user_id
AND w.currency = n.currency

WHEN MATCHED THEN
UPDATE SET
    w.balance = n.net_amount,
    w.updated_at = current_timestamp();

-- COMMAND ----------

-- ============================================================
-- RUNNING BALANCE VIEW
-- ============================================================

WITH ledger_view AS (

    SELECT
        txn_id,
        txn_timestamp,
        from_user AS user_id,
        from_currency AS currency,
        -from_amount AS amount
    FROM banking.bank_core.transactions
    WHERE from_user IS NOT NULL

    UNION ALL

    SELECT
        txn_id,
        txn_timestamp,
        to_user AS user_id,
        to_currency AS currency,
        to_amount AS amount
    FROM banking.bank_core.transactions
    WHERE to_user IS NOT NULL
),

balance_calc AS (

    SELECT
        txn_id,
        txn_timestamp,
        user_id,
        currency,
        amount,

        SUM(amount) OVER (
            PARTITION BY user_id, currency
            ORDER BY txn_timestamp, txn_id
        ) AS running_balance

    FROM ledger_view
)

SELECT
    txn_id,
    currency,

    CASE
        WHEN amount < 0 THEN ABS(amount)
        ELSE 0
    END AS debit,

    CASE
        WHEN amount > 0 THEN amount
        ELSE 0
    END AS credit,

    running_balance,
    txn_timestamp

FROM balance_calc
WHERE user_id = 1
ORDER BY txn_timestamp DESC;


-- COMMAND ----------

-- Displays latest transactions
SELECT
    txn_id,
    from_user,
    to_user,
    from_currency,
    to_currency,
    ROUND(from_amount, 2) AS from_amount,
    ROUND(to_amount, 2)   AS to_amount,
    fx_rate,
    txn_timestamp
FROM transactions
ORDER BY to_user;

-- COMMAND ----------

-- Displays transactions as Debit / Credit entries
SELECT
    txn_id,
    user_id,
    currency,

    ROUND(
        CASE WHEN entry_type = 'Debit' THEN ABS(amount) END
    , 2) AS debit,

    ROUND(
        CASE WHEN entry_type = 'Credit' THEN ABS(amount) END
    , 2) AS credit

FROM (

    -- Debit entry (sender)
    SELECT
        txn_id,
        from_user AS user_id,
        from_currency AS currency,
        from_amount AS amount,
        'Debit' AS entry_type
    FROM transactions
    WHERE from_user IS NOT NULL

    UNION ALL

    -- Credit entry (receiver)
    SELECT
        txn_id,
        to_user AS user_id,
        to_currency AS currency,
        to_amount AS amount,
        'Credit' AS entry_type
    FROM transactions
    WHERE to_user IS NOT NULL

) x

ORDER BY user_id;

-- COMMAND ----------

WITH ledger_view AS (

    SELECT
        txn_id,
        txn_timestamp,
        from_user AS user_id,
        from_currency AS currency,
        -from_amount AS amount
    FROM banking.bank_core.transactions
    WHERE from_user IS NOT NULL

    UNION ALL

    SELECT
        txn_id,
        txn_timestamp,
        to_user AS user_id,
        to_currency AS currency,
        to_amount AS amount
    FROM banking.bank_core.transactions
    WHERE to_user IS NOT NULL
),

balance_calc AS (

    SELECT
        txn_id,
        txn_timestamp,
        user_id,
        currency,
        amount,

        SUM(amount) OVER (
            PARTITION BY user_id, currency
            ORDER BY txn_timestamp, txn_id 
        ) AS running_balance

    FROM ledger_view
)

SELECT
    txn_id,
    user_id,
    currency,

    CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END AS debit,
    CASE WHEN amount > 0 THEN amount ELSE 0 END AS credit,

    running_balance

FROM balance_calc
WHERE user_id = 2
ORDER BY txn_timestamp DESC;

-- COMMAND ----------

-- Auto-create missing wallets based on transactions
MERGE INTO wallets w
USING (

    SELECT DISTINCT
        to_user   AS user_id,
        to_currency AS currency
    FROM transactions

) t

ON w.user_id = t.user_id
AND w.currency = t.currency

WHEN NOT MATCHED THEN
INSERT (
    wallet_id,
    user_id,
    currency,
    balance,
    updated_at
)
VALUES (
    CONCAT('W', t.user_id, t.currency),  -- wallet id generator
    t.user_id,
    t.currency,
    0,
    current_timestamp()
);

-- COMMAND ----------

-- Displays live wallet balances
SELECT
    user_id,
    currency,
    ROUND(balance, 2) AS balance
FROM wallets
ORDER BY user_id;

-- COMMAND ----------

