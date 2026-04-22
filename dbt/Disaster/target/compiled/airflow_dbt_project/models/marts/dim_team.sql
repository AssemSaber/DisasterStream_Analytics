



    with new_response_team_name as (
        select distinct response_team_name
        from "disaster"."intermediate"."inter_disaster"
        
        where response_team_name not in (select response_team_name from "disaster"."marts"."dim_team")
        
    ),

    max_sk as ( 
        
            select coalesce(max(team_id), 0) as max_key from "disaster"."marts"."dim_team"
        
    ),

    numbered_new as (
        select 
            (select max_key from max_sk ) + 
            row_number() over (order by response_team_name) as team_id,
            response_team_name
        from new_response_team_name
    )

    
        select team_id, response_team_name from "disaster"."marts"."dim_team"
        union all
        select team_id, response_team_name from numbered_new
    

