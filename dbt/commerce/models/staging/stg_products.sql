select 
id as category_id,
name as product_name,
category,
price 
from {{source('raw_data','products')}}
