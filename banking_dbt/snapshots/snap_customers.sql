{% snapshot snap_customers %}

{{
   config(
       target_database='CORE_BANKING_DB',
       target_schema='SNAPSHOTS',
       unique_key='customer_id',
       strategy='check',
       check_cols=['firstname', 'lastname', 'email']
   )
}}

SELECT * FROM {{ ref('stg_customers') }}

{% endsnapshot %}