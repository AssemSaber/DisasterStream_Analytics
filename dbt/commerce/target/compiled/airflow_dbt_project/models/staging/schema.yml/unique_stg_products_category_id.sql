
    
    

select
    category_id as unique_field,
    count(*) as n_records

from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_products
where category_id is not null
group by category_id
having count(*) > 1


