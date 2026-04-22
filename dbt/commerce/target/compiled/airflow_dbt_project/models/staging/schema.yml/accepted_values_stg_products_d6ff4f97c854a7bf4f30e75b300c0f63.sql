
    
    

with all_values as (

    select
        category as value_field,
        count(*) as n_records

    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_products
    group by category

)

select *
from all_values
where value_field not in (
    'Electornoics','Furniture','Accessories'
)


