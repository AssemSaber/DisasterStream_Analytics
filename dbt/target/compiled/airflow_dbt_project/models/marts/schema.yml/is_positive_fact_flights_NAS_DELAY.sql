
    select count(*) as records 
    from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.fact_flights
    where NAS_DELAY< 0
    having count(*)>0
