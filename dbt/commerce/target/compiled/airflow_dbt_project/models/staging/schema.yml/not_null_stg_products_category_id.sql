
    
    



select category_id
from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_products
where category_id is null


