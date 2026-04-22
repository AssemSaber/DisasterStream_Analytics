
  
    

        create or replace transient table GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart.dim_customer  as
        (select 
id,
customer_name,
email,
country
from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_customers
        );
      
  