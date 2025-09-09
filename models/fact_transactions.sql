{{ config(
    materialized='table',
    unique_key='transaction_id'
) }}

SELECT
    -- Transaction identifiers
    t.transaction_id,
    
    -- Foreign keys to dimensions
    t.debtor_customer_id,
    t.creditor_customer_id,
    t.payment_method_id,
    DATE(t.created_at) as transaction_date_key,
    
    -- Measures/Facts
    t.amount,
    t.currency,
    
    -- Transaction attributes
    t.status,
    t.reference,
    
    -- Calculated measures
    CASE 
        WHEN t.status = 'completed' THEN t.amount
        ELSE 0
    END as completed_amount,
    
    CASE 
        WHEN t.status = 'failed' THEN t.amount
        ELSE 0
    END as failed_amount,
    
    CASE 
        WHEN t.status = 'pending' THEN t.amount
        ELSE 0
    END as pending_amount,
    
    -- Status flags
    CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END as is_completed,
    CASE WHEN t.status = 'failed' THEN 1 ELSE 0 END as is_failed,
    CASE WHEN t.status = 'pending' THEN 1 ELSE 0 END as is_pending,
    
    -- Time dimensions
    t.created_at,
    t.updated_at,
    EXTRACT(HOUR FROM t.created_at) as transaction_hour,
    EXTRACT(DAYOFWEEK FROM t.created_at) as transaction_day_of_week,
    
    -- Additional attributes from related dimensions
    dc.customer_type as debtor_customer_type,
    dc.risk_profile as debtor_risk_profile,
    cc.customer_type as creditor_customer_type,
    cc.risk_profile as creditor_risk_profile,
    pm.method_type as payment_method_type,
    
    CURRENT_TIMESTAMP() as dbt_updated_at

FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('dim_customers') }} dc ON t.debtor_customer_id = dc.customer_id
LEFT JOIN {{ ref('dim_customers') }} cc ON t.creditor_customer_id = cc.customer_id
LEFT JOIN {{ ref('dim_payment_methods') }} pm ON t.payment_method_id = pm.payment_method_id