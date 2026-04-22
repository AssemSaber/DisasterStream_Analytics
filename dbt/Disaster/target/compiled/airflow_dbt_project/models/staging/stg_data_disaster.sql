SELECT 
  disaster_id,
  disaster_mapped,
  location,
  deaths_number,
  injured_number,
  families_affected_number,
  response_team_name,
  response_time_hours,
  "year",
  "month",
  "day",
  isDeleted,
  source_time_ms,
  operation_type
from "disaster"."raw_data"."disaster_events"