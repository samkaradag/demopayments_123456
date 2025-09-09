{{ config(materialized='view') }}

SELECT
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.payment_method_id') as payment_method_id,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.customer_id') as customer_id,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.method_type') as method_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(_airbyte_data, '$.details'), '$.card_number') as masked_card_number,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(_airbyte_data, '$.details'), '$.expiration_date') as expiration_date,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(_airbyte_data, '$.details'), '$.bank_code') as bank_code,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(_airbyte_data, '$.details'), '$.account_number') as masked_account_number,
    CAST(JSON_EXTRACT_SCALAR(_airbyte_data, '$.is_default') AS BOOL) as is_default,
    TIMESTAMP(JSON_EXTRACT_SCALAR(_airbyte_data, '$.created_at')) as created_at,
    TIMESTAMP(JSON_EXTRACT_SCALAR(_airbyte_data, '$.updated_at')) as updated_at,
    _airbyte_extracted_at,
    _airbyte_loaded_at
FROM {{ source('demo_miguel', 'airbyte_raw_payment_methods') }}