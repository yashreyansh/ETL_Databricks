{{
    config(
        materialized='incremental',
        
        incremental_strategy='append'  
    )
}}

{# unique_key='custkey',   #  SCD2 append updated rows as well as duplicates #}

with customer_renamed as (
    select
        c_custkey AS custkey,
        c_name AS cust_name,
        c_address AS cust_address,
        c_nationkey AS nationkey,
        c_phone AS cust_phone,
        c_acctbal AS cust_acctbal,
        c_mktsegment AS cust_mktsegment,
        c_comment AS cust_comment,
        status ,
        COALESCE(updated_on, '2000-01-01'::timestamp) as updated_on,
        COALESCE(UPDATED_BY,'ADMIN') as UPDATED_BY,
        created_on ,
        created_by 
    from 
        {{ source('databricks_schema_alias','customer_raw')}}

    {% if is_incremental() %}
where
    updated_on>(
            select COALESCE(max(updated_on), '2000-01-01'::timestamp) 
            from {{ this }} 
            )
    {% endif %}
)

select * from customer_renamed
