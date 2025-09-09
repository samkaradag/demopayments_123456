{{ config(materialized='view') }}

SELECT
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.customer_id') as customer_id,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.customer_type') as customer_type,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.email') as email,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.phone_number') as phone_number,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(_airbyte_data, '$.address'), '$.street') as street,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(_airbyte_data, '$.address'), '$.city') as city,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT(_airbyte_data, '$.address'), '$.country') as country,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.kyc_status') as kyc_status,
    JSON_EXTRACT_SCALAR(_airbyte_data, '$.risk_profile') as risk_profile,
    TIMESTAMP(JSON_EXTRACT_SCALAR(_airbyte_data, '$.created_at')) as created_at,
    TIMESTAMP(JSON_EXTRACT_SCALAR(_airbyte_data, '$.updated_at')) as updated_at,
    _airbyte_extracted_at,
    _airbyte_loaded_at
FROM {{ source('demo_miguel', 'airbyte_raw_customers') }}