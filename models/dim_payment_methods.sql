{{ config(
    materialized='table',
    unique_key='payment_method_id'
) }}

WITH payment_methods_with_row_number AS (
    SELECT 
        payment_method_id,
        customer_id,
        method_type,
        masked_card_number,
        expiration_date,
        bank_code,
        masked_account_number,
        is_default,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY payment_method_id ORDER BY updated_at DESC) as rn
    FROM {{ ref('stg_payment_methods') }}
)

SELECT
    payment_method_id,
    customer_id,
    method_type,
    CASE 
        WHEN method_type IN ('credit_card', 'debit_card', 'card') THEN 'Card'
        WHEN method_type = 'bank_transfer' THEN 'Bank Transfer'
        WHEN method_type = 'digital_wallet' THEN 'Digital Wallet'
        ELSE 'Other'
    END as method_category,
    masked_card_number,
    expiration_date,
    bank_code,
    masked_account_number,
    is_default,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM payment_methods_with_row_number
WHERE rn = 1