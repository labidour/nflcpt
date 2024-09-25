select 
     id as qb_id
    ,name as qb
    ,position as qb_pos
    ,team as qb_team
    ,opp as qb_opp
    ,game as qb_game
    ,salary as qb_sal
    ,fpts as qb_fpts
    ,value as qb_val
from {{ref("roster")}}
where position like 'QB'
and value > (select avg(value) from {{ref("roster")}}
where position like 'QB')