select
id as order_id,
customer_id,
order_date,
total_amount,
lower(status) as status
from {{source('raw_data','orders')}}