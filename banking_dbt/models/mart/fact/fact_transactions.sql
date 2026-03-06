{{
  config(
    materialized = 'incremental',
    unique_key = 'transaction_id',
    )
}}

SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.amount,
    t.transaction_type,
    t.recipient_account_id,
    t.status,
    t.create_date,
    CURRENT_TIMESTAMP AS fact_updated_at
FROM {{ ref('stg_transactions') }} t
LEFT JOIN {{ ref('dim_accounts') }} a 
    ON t.account_id = a.account_id
    AND t.create_date >= a.valid_from 
    AND (t.create_date < a.valid_to OR a.valid_to IS NULL)

{% if is_incremental() %}

  WHERE t.create_date > (SELECT MAX(create_date) FROM {{ this }})

{% endif %}