{{ 
    config(
    materialized='incremental',
    unique_key='response_team_name'
    ) 
}}

{{ new_records('team_id','response_team_name') }}