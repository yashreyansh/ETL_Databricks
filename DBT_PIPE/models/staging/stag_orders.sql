
{{
    config(
        materialized='incremental',
        unique_key='order_id',
        
    )
}}
{# incremental_strategy='append'    SCD1 update on uniqueKey #}


with renamed_orders as (
    select o_orderkey as orderkey,
        o_custkey as custkey,
            o_orderstatus as orderstatus,
                o_totalprice as totalprice,
                    o_orderdate as orderdate,
                        o_orderpriority as orderpriority,
                            o_clerk as clerk,
                                o_shippriority as shippriority,
                                    o_comment as order_comment,
                                        ORDER_EXECUTION_STATUS ,
                            COALESCE(updated_on, '2000-01-01'::timestamp) as updated_on,
                                        created_on,
                                    COALESCE(UPDATED_BY,'ADMIN') as UPDATED_BY,
                                            CREATED_BY
        from 
        {{source('databricks_schema_alias','orders_raw')}}

    {% if is_incremental() %}
    where updated_on>(
                        select max(updated_on) from {{ this }}
                        )
    {% endif %}

    )


select * From  renamed_orders 

