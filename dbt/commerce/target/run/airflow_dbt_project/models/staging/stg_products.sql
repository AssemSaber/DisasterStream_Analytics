
  create or replace   view GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_products
  
   as (
    select 
id as category_id,
name as product_name,
category,
price 
from GP.RAW.products
  );

