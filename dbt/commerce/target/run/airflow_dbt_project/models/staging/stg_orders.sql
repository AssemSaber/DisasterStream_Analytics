
  create or replace   view GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_orders
  
   as (
    select
id as order_id,
customer_id,
order_date,
total_amount,
lower(status) as status
from GP.RAW.orders
  );

