{{ 
    config(
    materialized='incremental',
    unique_key='location'
    ) 
}}

with new_locations as (
    select distinct location
    from {{ ref('inter_disaster') }}
    {% if is_incremental() %}
    where location not in (select location from {{ this }})
    {% endif %}
),
-- merge not applicable in this postgre version
-- so, this the other approach

max_sk as (  -- here we get the max surrogate key in case of weather there is table (start with the end) or not ( start with zero)
    {% if is_incremental() %}
        select coalesce(max(location_id), 0) as max_key from {{ this }}
    {% else %}
        select 0 as max_key
    {% endif %}
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
{% if is_incremental() %}
    select location_id, location from {{ this }}
    union all
    select location_id, location from numbered_new
{% else %}
    select location_id, location from numbered_new
{% endif %}

-- is incremental >> there is a table
-- else >> no table found