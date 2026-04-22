select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select destination_id as from_field
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.fact_flights
    where destination_id is not null
),

parent as (
    select destination_id as to_field
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_destination
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test