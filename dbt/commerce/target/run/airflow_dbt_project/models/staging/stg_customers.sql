
  create or replace   view GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_customers
  
   as (
    select 
id,
name as customer_name,
email,
country
from GP.RAW.customers
  );

