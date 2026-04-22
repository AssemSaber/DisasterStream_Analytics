


with distinct_values as(
    select
    distinct origin_city as city,origin_state as state
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_flight_status
)
select row_number() over(order by city) as origin_id,
    city,
    state
    from distinct_values

