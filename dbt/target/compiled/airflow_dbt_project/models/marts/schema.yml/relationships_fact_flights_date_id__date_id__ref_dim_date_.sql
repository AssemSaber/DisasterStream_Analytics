
    
    

with child as (
    select date_id as from_field
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.fact_flights
    where date_id is not null
),

parent as (
    select date_id as to_field
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_date
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


