select 
order_id,
product_id,
sum(quantity*unit_price) as revenue
from GP.RAW.order_items
group by order_id,product_id