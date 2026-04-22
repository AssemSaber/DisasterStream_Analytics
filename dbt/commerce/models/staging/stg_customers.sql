select 
id,
name as customer_name,
email,
country
from {{source('raw_data','customers')}}