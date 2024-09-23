-- Rank dsts by fpts per team
    SELECT
        id AS dst_id,
        name AS dst,
        'DST' AS dst_pos,
        team AS dst_team,
        opp AS dst_opp,
        game AS dst_game,
        salary AS dst_sal,
        fpts AS dst_fpts,
        value AS dst_val,
        --ROW_NUMBER() OVER (PARTITION BY team ORDER BY fpts DESC) AS rank_in_team
    FROM NFLMAIN.PUBLIC.roster
    where position like 'DST'
and value > (select avg(value) from {{ref("roster")}}
where position like 'DST')


