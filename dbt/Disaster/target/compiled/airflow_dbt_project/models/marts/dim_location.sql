

with new_locations as (
    select distinct location
    from "disaster"."intermediate"."inter_disaster"
    
    where location not in (select location from "disaster"."marts"."dim_location")
    
),
-- merge not applicable in this postgre version
-- so, this the other approach

max_sk as (  -- here we get the max surrogate key in case of weather there is table (start with the end) or not ( start with zero)
    
        select coalesce(max(location_id), 0) as max_key from "disaster"."marts"."dim_location"
    
),
-- add the max_key (0 or last_value) + row_number 
numbered_new as (
    select 
        (select max_key from max_sk ) + 
        row_number() over (order by location) as location_id,
        location
    from new_locations
)

-- if there is a table, we make union all
-- if there is not a table , we depend on the numbered_new started with 0 

    select location_id, location from "disaster"."marts"."dim_location"
    union all
    select location_id, location from numbered_new


-- is incremental >> there is a table
-- else >> no table found