
      
  
    

  create  table "disaster"."marts"."dim_teams"
  as (
    with rnk_teams as(
    select row_number() over(order by disaster_id) as rnk , response_team_name 
    from "disaster"."intermediate"."inter_disaster"
)  
select response_team_name from rnk_teams 
    where rnk=1
  );
  
  