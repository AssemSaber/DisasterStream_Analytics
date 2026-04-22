
  create or replace   view GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_order_itmes
  
   as (
    select 
id,
order_id,
product_id,
quantity,
unit_price,
quantity*unit_price as revenue
from GP.RAW.order_items
  );

