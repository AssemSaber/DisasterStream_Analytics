
  create or replace   view GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_order_items
  
   as (
    select 
order_id,
product_id,
sum(quantity*unit_price) as revenue
from GP.RAW.order_items
group by order_id,product_id
  );

