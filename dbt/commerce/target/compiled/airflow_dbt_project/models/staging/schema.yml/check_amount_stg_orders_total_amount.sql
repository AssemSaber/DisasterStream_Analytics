
    select count(*) as records 
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_orders
    where total_amount< 0
    having count(*)>0
