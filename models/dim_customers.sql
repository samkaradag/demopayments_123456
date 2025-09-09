{{ config(
    materialized='table',
    unique_key='customer_id'
) }}

WITH customers_with_row_number AS (
    SELECT 
        customer_id,
        customer_type,
        email,
        phone_number,
        street,
        city,
        country,
        kyc_status,
        risk_profile,
        created_at,
        updated_at,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) as rn
    FROM {{ ref('stg_customers') }}
)

SELECT
    customer_id,
    customer_type,
    email,
    phone_number,
    street,
    city,
    country,
    kyc_status,
    risk_profile,
    created_at,
    updated_at,
    CURRENT_TIMESTAMP() as dbt_updated_at
FROM customers_with_row_number
WHERE rn = 1