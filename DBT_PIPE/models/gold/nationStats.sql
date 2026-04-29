
-- get top 10 nation wise orders in last month

-- plan?
-- Join orders and customer and nationKey on customer
-- group the nationKey

with customer_order_join as (

    select totalprice, nationKey, orderstatus
    from {{ ref("silver_customers") }} cust INNER Join  
        {{ ref("silver_order")}} ord ON ord.custkey=cust.custkey

   where order_date >= add_months(date_trunc('month', current_date()), -1) AND order_date <  date_trunc('month', current_date())
    --and orderstatus = 'F'  -- fullfilled
) ,
con_nation as (
    select cust.totalprice, cust.nationKey, cust.orderstatus,nation.country_name
     from customer_order_join cust LEFT JOIN {{ref("nationMetadata")}} nation
    ON nation.nation_key=cust.nationKey
)

select nationKey, country_name,
SUM(CASE WHEN orderstatus='F' THEN totalprice ELSE 0 END ) as fullfilled_value,
SUM(CASE WHEN orderstatus='0' THEN totalprice ELSE 0 END ) as open_order_value
 From con_nation
group by nationKey,country_name order by fullfilled_value desc, country_name 

