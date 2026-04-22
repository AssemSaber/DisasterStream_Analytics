
    
    

select
    id as unique_field,
    count(*) as n_records

from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_customers
where id is not null
group by id
having count(*) > 1


