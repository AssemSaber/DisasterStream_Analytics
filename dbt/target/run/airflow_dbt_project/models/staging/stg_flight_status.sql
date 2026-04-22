
  
    

        create or replace transient table GP.-- u get the defualt target schema provided in profiles.yml-- else >>> the customized the schema

        staging.stg_flight_status  as
        (select 
    DATE_FROM_PARTS(year, month, day_of_month) AS flight_date,
    
    lower(op_unique_carrier)
 as Airline_Code,
    op_carrier_fl_num::INT as Flight_Number,
    
    lower(origin)
 as Origin_Airport_Code,
    
    lower(origin_city_name)
 as Origin_City,
    
    lower(origin_state_nm)
 as Origin_State,
    
    lower(dest)
 as Destination_Airport_Code,
    
    lower(dest_city_name)
 as Destination_City,
    
    lower(dest_state_nm)
 as Destination_State,
    CAST(crs_dep_time AS INT) as Scheduled_Departure_Time,
    CAST(dep_time AS INT) as Actual_Departure_Time,
    dep_delay::INT as Departure_Delay,
    taxi_out::INT as Taxi_Out_Time,
    CAST(wheels_off AS INT) as Wheels_Off_Time,
    CAST(wheels_on AS INT) as Wheels_On_Time,
    taxi_in::INT as Taxi_In_Time,
    CAST(crs_arr_time AS INT) as Scheduled_Arrival_Time,
    CAST(arr_time AS INT)as Actual_Arrival_Time,
    arr_delay::INT as Arrival_Delay,
    CASE cancelled WHEN 0 THEN 'No'  WHEN 1 THEN 'Yes' END as is_cancelled,
    CASE cancellation_code WHEN 'A' THEN 'Carrier'  WHEN 'B' THEN 'Weather'  WHEN 'C' THEN 'National Air System'  WHEN 'D' THEN 'Security'  ELSE 'still available' END as Cancellation_Reason,
    CASE diverted WHEN 0 THEN 'No'  WHEN 1 THEN 'Yes' END as is_diverted,
    crs_elapsed_time::INT as Scheduled_Flight_Duration,
    actual_elapsed_time::INT as Actual_Flight_Duration,
    air_time,
    distance,
    carrier_delay,
    weather_delay,
    nas_delay,
    security_delay,
    late_aircraft_delay,
    isDelete,
    CASE operation WHEN 'c' THEN 'Create'  WHEN 'u' THEN 'Update'  WHEN 'd' THEN 'Delete'  WHEN 'r' THEN 'Snapshot'  ELSE 'Unknown' END as operation_type,
    TO_TIMESTAMP_LTZ(event_time / 1000) as event_time,
    ingestion_time  as ingestion_datetime
from GP.RAW.flights
        );
      
  