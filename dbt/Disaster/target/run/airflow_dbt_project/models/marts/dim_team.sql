
      
        
            delete from "disaster"."marts"."dim_team"
            where (
                response_team_name) in (
                select (response_team_name)
                from "dim_team__dbt_tmp204848126352"
            );

        
    

    insert into "disaster"."marts"."dim_team" ("team_id", "response_team_name")
    (
        select "team_id", "response_team_name"
        from "dim_team__dbt_tmp204848126352"
    )
  