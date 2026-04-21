

{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with column_renamed_orders as (

    select orderkey,	custkey,	orderstatus,
    totalprice,
    CASE WHEN totalprice>=100000 THEN 'Y'
        ELSE 'N' END as HIGH_VALUE_ORDER,
    CASE WHEN orderdate< CAST('2000-01-01' as timestamp)
        THEN orderdate + INTERVAL 30 YEARS
        ELSE orderdate END as ORDER_date,
        orderpriority,	clerk,	
    CASE WHEN HIGH_VALUE_ORDER='Y' THEN 1
        ELSE 0 END AS shippriority,
        order_comment, 
        ORDER_EXECUTION_STATUS,	updated_on,	created_on,	UPDATED_BY,	CREATED_BY
    from {{ref("stag_orders")}}

)

select * from column_renamed_orders

{% if is_incremental() %}
    WHERE updated_on > 
        (SELECT max(updated_on) FROM {{ this }} )

{% endif %}