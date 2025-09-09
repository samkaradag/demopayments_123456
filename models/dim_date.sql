{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT 
        date_day
    FROM (
        SELECT 
            DATE_ADD('2020-01-01', INTERVAL row_num DAY) as date_day
        FROM (
            SELECT 
                ROW_NUMBER() OVER() - 1 as row_num
            FROM UNNEST(GENERATE_ARRAY(1, 3653)) as x  -- ~10 years of dates
        )
    )
    WHERE date_day <= DATE_ADD(CURRENT_DATE(), INTERVAL 1 YEAR)
)

SELECT
    date_day as date_key,
    date_day,
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(QUARTER FROM date_day) as quarter,
    EXTRACT(MONTH FROM date_day) as month,
    EXTRACT(WEEK FROM date_day) as week,
    EXTRACT(DAYOFWEEK FROM date_day) as day_of_week,
    EXTRACT(DAYOFYEAR FROM date_day) as day_of_year,
    FORMAT_DATE('%B', date_day) as month_name,
    FORMAT_DATE('%A', date_day) as day_name,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE 
        ELSE FALSE 
    END as is_weekend,
    CASE 
        WHEN EXTRACT(DAYOFWEEK FROM date_day) BETWEEN 2 AND 6 THEN TRUE 
        ELSE FALSE 
    END as is_weekday,
    CONCAT(EXTRACT(YEAR FROM date_day), '-Q', EXTRACT(QUARTER FROM date_day)) as quarter_name,
    CONCAT(EXTRACT(YEAR FROM date_day), '-', LPAD(CAST(EXTRACT(MONTH FROM date_day) AS STRING), 2, '0')) as year_month
FROM date_spine