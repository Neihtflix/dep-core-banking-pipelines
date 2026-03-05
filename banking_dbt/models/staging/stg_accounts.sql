{{ config(materialized='view') }}

with ranked as (
    select
        v:account_id::string as account_id,
        v:customer_id::string as customer_id,
        v:account_type::string as account_type,
        v:balance::float as balance,
        v:currency::string as currency,
        v:create_date::timestamp as create_date,
        current_timestamp as stg_updated_at,
        row_number() over (
            partition by v:account_id::string
            order by v:create_date desc
        ) as rn
    from {{ source('bronze', 'accounts') }}
)

select 
    account_id,
    customer_id,
    account_type,
    balance,
    currency,
    create_date,
    stg_updated_at
from 
    ranked
where rn = 1