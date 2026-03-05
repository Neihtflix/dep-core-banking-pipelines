{% snapshot snap_accounts %}

{{
   config(
       target_database='CORE_BANKING_DB',
       target_schema='SNAPSHOTS',
       unique_key='account_id',
       strategy='check',
       check_cols=['customer_id', 'account_type', 'balance']
   )
}}

SELECT * FROM {{ ref('stg_accounts') }}

{% endsnapshot %}