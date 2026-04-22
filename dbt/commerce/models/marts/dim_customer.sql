select 
id,
customer_name,
email,
country
from {{ref('stg_customers')}}