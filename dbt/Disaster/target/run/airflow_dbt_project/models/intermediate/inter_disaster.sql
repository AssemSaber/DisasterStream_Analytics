
  
    

  create  table "disaster"."intermediate"."inter_disaster__dbt_tmp"
  as (
    SELECT 
  disaster_id,
  disaster_mapped,
  location,
  deaths_number,
  injured_number,
  families_affected_number,
  response_team_name,
  response_time_hours,
  isDeleted,
  to_timestamp(source_time_ms / 1000.0) AS ingestion_time,
  operation_type,
  make_date("year", "month", "day") AS disaster_date
FROM "disaster"."staging"."stg_data_disaster"
  );
  