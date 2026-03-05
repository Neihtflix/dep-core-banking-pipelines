{{ config(materialized='view') }}

select
    v:transaction_id::string as transaction_id,
    v:account_id::string as account_id,
    v:amount::float as amount,
    v:transaction_type::string as transaction_type,
    v:recipient_account_id::string as recipient_account_id,
    v:status::string as status,
    v:create_date::timestamp as create_date,
    current_timestamp as stg_updated_at
from {{ source('bronze', 'transactions') }}