select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with all_values as (

    select
        status as value_field,
        count(*) as n_records

    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_orders
    group by status

)

select *
from all_values
where value_field not in (
    'pending','completed','cancelled'
)



      
    ) dbt_internal_test