{{ config(materialized='table') }}

WITH customer_transaction_stats AS (
    SELECT
        f.debtor_customer_id as customer_id,
        COUNT(*) as total_transactions_as_debtor,
        SUM(f.amount) as total_amount_as_debtor,
        SUM(f.completed_amount) as completed_amount_as_debtor,
        AVG(f.amount) as avg_transaction_amount_as_debtor,
        MAX(f.created_at) as last_transaction_date,
        MIN(f.created_at) as first_transaction_date
    FROM {{ ref('fact_transactions') }} f
    GROUP BY f.debtor_customer_id
),

customer_payment_methods AS (
    SELECT
        customer_id,
        COUNT(*) as total_payment_methods,
        COUNT(CASE WHEN is_default THEN 1 END) as default_payment_methods
    FROM {{ ref('dim_payment_methods') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.customer_type,
    c.email,
    c.city,
    c.country,
    c.kyc_status,
    c.risk_profile,
    
    -- Transaction metrics
    COALESCE(cts.total_transactions_as_debtor, 0) as total_transactions,
    COALESCE(cts.total_amount_as_debtor, 0) as total_transaction_amount,
    COALESCE(cts.completed_amount_as_debtor, 0) as total_completed_amount,
    COALESCE(cts.avg_transaction_amount_as_debtor, 0) as avg_transaction_amount,
    
    -- Customer lifecycle
    c.created_at as customer_created_at,
    cts.first_transaction_date,
    cts.last_transaction_date,
    DATE_DIFF(CURRENT_DATE(), DATE(cts.last_transaction_date), DAY) as days_since_last_transaction,
    DATE_DIFF(DATE(cts.last_transaction_date), DATE(cts.first_transaction_date), DAY) as customer_lifetime_days,
    
    -- Payment methods
    COALESCE(cpm.total_payment_methods, 0) as total_payment_methods,
    COALESCE(cpm.default_payment_methods, 0) as default_payment_methods,
    
    -- Customer segmentation
    CASE 
        WHEN COALESCE(cts.total_transactions_as_debtor, 0) = 0 THEN 'No Transactions'
        WHEN COALESCE(cts.total_transactions_as_debtor, 0) = 1 THEN 'One-time Customer'
        WHEN COALESCE(cts.total_transactions_as_debtor, 0) BETWEEN 2 AND 5 THEN 'Occasional Customer'
        WHEN COALESCE(cts.total_transactions_as_debtor, 0) BETWEEN 6 AND 20 THEN 'Regular Customer'
        ELSE 'Power Customer'
    END as customer_segment,
    
    CASE 
        WHEN COALESCE(cts.total_amount_as_debtor, 0) = 0 THEN 'No Spend'
        WHEN COALESCE(cts.total_amount_as_debtor, 0) < 100 THEN 'Low Value'
        WHEN COALESCE(cts.total_amount_as_debtor, 0) < 1000 THEN 'Medium Value'
        WHEN COALESCE(cts.total_amount_as_debtor, 0) < 10000 THEN 'High Value'
        ELSE 'Premium Value'
    END as value_segment,
    
    CURRENT_TIMESTAMP() as dbt_updated_at

FROM {{ ref('dim_customers') }} c
LEFT JOIN customer_transaction_stats cts ON c.customer_id = cts.customer_id
LEFT JOIN customer_payment_methods cpm ON c.customer_id = cpm.customer_id