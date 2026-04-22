
      
        
            delete from "disaster"."marts"."dim_location"
            where (
                location) in (
                select (location)
                from "dim_location__dbt_tmp204848022592"
            );

        
    

    insert into "disaster"."marts"."dim_location" ("location_id", "location")
    (
        select "location_id", "location"
        from "dim_location__dbt_tmp204848022592"
    )
  