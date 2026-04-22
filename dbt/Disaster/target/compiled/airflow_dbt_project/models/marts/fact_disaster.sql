

select
disaster_id,
dd.date_id as date_key,
ddt.disaster_type_id as disaster_type_id,
dl.location_id as location_id,
dt.team_id as team_id,
deaths_number,
injured_number,
families_affected_number,
response_time_hours
from "disaster"."intermediate"."inter_disaster" as inter 
join "disaster"."marts"."dim_date" as  dd
on inter.disaster_date=dd.full_date
join "disaster"."marts"."dim_disaster_type" as ddt 
on inter.disaster_mapped=ddt.disaster_mapped
join "disaster"."marts"."dim_location" as dl 
on inter.location=dl.location
join "disaster"."marts"."dim_team" as dt 
on inter.response_team_name=dt.response_team_name