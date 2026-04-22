{{ 
    config(
    materialized='incremental',
    unique_key='disaster_type_id'
    ) 
}}

{{ new_records('disaster_type_id','disaster_mapped') }}