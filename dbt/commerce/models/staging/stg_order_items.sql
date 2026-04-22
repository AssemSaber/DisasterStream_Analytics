select 
order_id,
product_id,
sum(quantity*unit_price) as revenue
from {{source('raw_data','order_items')}}
group by order_id,product_id
