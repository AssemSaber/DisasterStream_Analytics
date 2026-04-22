
    
    

select
    order_id as unique_field,
    count(*) as n_records

from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_orders
where order_id is not null
group by order_id
having count(*) > 1


