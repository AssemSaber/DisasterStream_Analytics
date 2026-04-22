



    with new_disaster_mapped as (
        select distinct disaster_mapped
        from "disaster"."intermediate"."inter_disaster"
        
        where disaster_mapped not in (select disaster_mapped from "disaster"."marts"."dim_disaster_type")
        
    ),

    max_sk as ( 
        
            select coalesce(max(disaster_type_id), 0) as max_key from "disaster"."marts"."dim_disaster_type"
        
    ),

    numbered_new as (
        select 
            (select max_key from max_sk ) + 
            row_number() over (order by disaster_mapped) as disaster_type_id,
            disaster_mapped
        from new_disaster_mapped
    )

    
        select disaster_type_id, disaster_mapped from "disaster"."marts"."dim_disaster_type"
        union all
        select disaster_type_id, disaster_mapped from numbered_new
    

