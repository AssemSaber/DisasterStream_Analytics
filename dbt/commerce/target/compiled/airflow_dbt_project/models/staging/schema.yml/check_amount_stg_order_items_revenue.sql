
    select count(*) as records 
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_order_items
    where revenue< 0
    having count(*)>0
