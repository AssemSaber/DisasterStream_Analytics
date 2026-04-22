
  
    

        create or replace transient table GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.fact_flights  as
        (
select
d_date.date_id as date_id,
sdt.time_id as Scheduled_Departure_Time_id,
adt.time_id as Actual_Departure_Time_id,
wot.time_id as Wheels_Off_Time_id,
wot2.time_id as Wheels_On_Time_id,
sat.time_id as Scheduled_Arrival_Time_id,
aat.time_id as Actual_Arrival_Time_id,
origin.origin_id as origin_id,
dest.destination_id as destination_id, 
fl_stat.status_id as status_id,
AIRLINE_CODE,
FLIGHT_NUMBER,
Departure_Delay,
Taxi_Out_Time,
Taxi_In_Time,
Arrival_Delay,
Scheduled_Flight_Duration,
Actual_Flight_Duration,
Air_Time,
Distance,
Carrier_Delay,
Weather_Delay,
NAS_Delay,
Security_Delay,
Late_Aircraft_Delay
from GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_flight_status as stg 
left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_date as d_date
on stg.flight_date=d_date.date_day

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_time as sdt
on stg.Scheduled_Departure_Time=sdt.hhmm

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_time as adt 
on stg.Actual_Departure_Time=adt.hhmm

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_time as wot
on stg.Wheels_Off_Time=wot.hhmm

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_time as wot2 
on stg.Wheels_On_Time=wot2.hhmm

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_time as sat
on stg.Scheduled_Arrival_Time=sat.hhmm

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_time as aat
on stg.Actual_Arrival_Time=aat.hhmm

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_origin as origin
on origin.city=stg.origin_city and origin.state=stg.origin_state 

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_destination as dest
on dest.city=stg.destination_city and dest.state=stg.destination_state 

left join GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        mart_flight.dim_flight_status as fl_stat 
on stg.IS_CANCELLED=fl_stat.IS_CANCELLED and stg.IS_DIVERTED=fl_stat.IS_DIVERTED
        );
      
  