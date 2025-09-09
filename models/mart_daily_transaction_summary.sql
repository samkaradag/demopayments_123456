{{ config(materialized='table') }}

SELECT
    f.transaction_date_key,
    d.date_day,
    d.year,
    d.month,
    d.quarter,
    d.month_name,
    d.day_name,
    d.is_weekend,
    
    -- Transaction counts
    COUNT(*) as total_transactions,
    SUM(f.is_completed) as completed_transactions,
    SUM(f.is_failed) as failed_transactions,
    SUM(f.is_pending) as pending_transactions,
    
    -- Transaction amounts
    SUM(f.amount) as total_amount,
    SUM(f.completed_amount) as total_completed_amount,
    SUM(f.failed_amount) as total_failed_amount,
    SUM(f.pending_amount) as total_pending_amount,
    
    -- Average amounts
    AVG(f.amount) as avg_transaction_amount,
    AVG(CASE WHEN f.is_completed = 1 THEN f.amount END) as avg_completed_amount,
    
    -- Success rate
    SAFE_DIVIDE(SUM(f.is_completed), COUNT(*)) * 100 as success_rate_percent,
    
    -- Currency breakdown
    COUNT(DISTINCT f.currency) as currencies_used,
    
    -- Customer metrics
    COUNT(DISTINCT f.debtor_customer_id) as unique_debtor_customers,
    COUNT(DISTINCT f.creditor_customer_id) as unique_creditor_customers,
    
    -- Payment method metrics
    COUNT(DISTINCT f.payment_method_id) as unique_payment_methods,
    
    CURRENT_TIMESTAMP() as dbt_updated_at

FROM {{ ref('fact_transactions') }} f
INNER JOIN {{ ref('dim_date') }} d ON f.transaction_date_key = d.date_key
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY f.transaction_date_key DESC