{{ config(materialized='table') }}

WITH latest AS (
    SELECT
        customer_id,
        firstname,
        lastname,
        email,
        create_date,
        dbt_valid_from AS valid_from,
        dbt_valid_to AS valid_to,
        CASE 
            WHEN dbt_valid_to IS NULL THEN TRUE
            ELSE FALSE
        END AS is_current
    FROM {{ ref('snap_customers') }}
)

SELECT * FROM latest