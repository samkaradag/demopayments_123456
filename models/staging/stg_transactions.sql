{{ config(materialized='view') }}

SELECT
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.transaction_id') as transaction_id,
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.amount') AS FLOAT64) as amount,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.currency') as currency,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.status') as status,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.payment_method_id') as payment_method_id,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.debtor_customer_id') as debtor_customer_id,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.creditor_customer_id') as creditor_customer_id,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.reference') as reference,
    TIMESTAMP(JSON_EXTRACT_SCALAR(_airbyte_data, '$.created_at')) as created_at,
    TIMESTAMP(JSON_EXTRACT_SCALAR(_airbyte_data, '$.updated_at')) as updated_at,
    _airbyte_extracted_at,
    _airbyte_loaded_at
FROM {{ source('demo_miguel', 'airbyte_raw_transactions') }}