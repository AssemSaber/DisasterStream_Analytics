select 
id,
order_id,
product_id,
quantity,
unit_price,
quantity*unit_price as revenue
from GP.RAW.order_items