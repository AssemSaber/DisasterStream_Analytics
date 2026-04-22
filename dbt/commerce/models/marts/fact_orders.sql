-- with order_itmes as (
-- select 
-- order_id,
-- product_id,
-- sum(quantity*unit_price) as revenue
-- from {{source('raw_data','order_items')}}
-- group by revenue

-- ),
--  orders as(
--     select
--     id,
--     customer_id,
--     order_date,
--     total_amount,
--     status
--     from 

-- ),

    select 
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    oi.revenue
    from {{ref('stg_orders')}} o left join  {{ref('stg_order_items')}} oi
    on o.order_id=oi.order_id 

