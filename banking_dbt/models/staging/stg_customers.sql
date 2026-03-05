{{ config(materialized='view') }}

with ranked as (
    select
        v:customer_id::string as customer_id,
        v:firstname::string as firstname,
        v:lastname::string as lastname,
        v:email::string as email,
        v:create_date::timestamp as create_date,
        current_timestamp() as stg_updated_at,
        row_number() over (
            partition by v:customer_id::string
            order by v:create_date desc
        ) as rn
    from {{ source('bronze', 'customers') }}
)

select 
    customer_id,
    firstname,
    lastname,
    email,
    create_date,
    stg_updated_at
from 
    ranked
where rn = 1