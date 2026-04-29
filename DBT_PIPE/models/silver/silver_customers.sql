{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with column_renamed_customers as (

    select custkey,	cust_name,	cust_address,	nationkey,
    CONCAT('XXX-XXX-',RIGHT(cust_phone,4)) as CUST_PHONE,
    	cust_acctbal	,cust_mktsegment
    cust_comment,	status,	updated_on,	UPDATED_BY,	created_on,	created_by

    from 
    {{ ref("stag_customers") }}

)

select * From 

    column_renamed_customers 


{% if is_incremental() %}
    where updated_on >
     ( SELECT MAX(updated_on) from {{ this }} )
{% endif %}

