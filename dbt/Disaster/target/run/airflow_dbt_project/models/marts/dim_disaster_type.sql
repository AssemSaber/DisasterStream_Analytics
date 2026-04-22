
      
        
            delete from "disaster"."marts"."dim_disaster_type"
            where (
                disaster_type_id) in (
                select (disaster_type_id)
                from "dim_disaster_type__dbt_tmp204846927324"
            );

        
    

    insert into "disaster"."marts"."dim_disaster_type" ("disaster_type_id", "disaster_mapped")
    (
        select "disaster_type_id", "disaster_mapped"
        from "dim_disaster_type__dbt_tmp204846927324"
    )
  