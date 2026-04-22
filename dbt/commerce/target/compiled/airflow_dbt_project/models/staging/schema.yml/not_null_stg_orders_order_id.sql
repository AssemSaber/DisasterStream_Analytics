
    
    



select order_id
from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_orders
where order_id is null


