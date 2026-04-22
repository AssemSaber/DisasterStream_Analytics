
      
        
            delete from "disaster"."marts"."fact_disaster"
            where (
                disaster_id) in (
                select (disaster_id)
                from "fact_disaster__dbt_tmp204905274390"
            );

        
    

    insert into "disaster"."marts"."fact_disaster" ("disaster_id", "date_key", "disaster_type_id", "location_id", "team_id", "deaths_number", "injured_number", "families_affected_number", "response_time_hours")
    (
        select "disaster_id", "date_key", "disaster_type_id", "location_id", "team_id", "deaths_number", "injured_number", "families_affected_number", "response_time_hours"
        from "fact_disaster__dbt_tmp204905274390"
    )
  