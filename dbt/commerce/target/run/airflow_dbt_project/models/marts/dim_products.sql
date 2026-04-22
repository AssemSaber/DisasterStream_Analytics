
  
    

        create or replace transient table GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart.dim_products  as
        (select 
category_id,
product_name,
category,
price 
from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_products
        );
      
  