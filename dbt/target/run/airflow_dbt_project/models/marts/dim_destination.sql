
  
    

        create or replace transient table GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_destination  as
        (


with distinct_values as(
    select
    distinct destination_city as city,destination_state as state
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_flight_status
)
select row_number() over(order by city) as destination_id,
    city,
    state
    from distinct_values


        );
      
  