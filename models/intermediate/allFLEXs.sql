-- Rank RBs by fpts per team
    SELECT
        id AS flex_id,
        name AS flex,
        'FLEX' AS flex_pos,
        team AS flex_team,
        opp AS flex_opp,
        game AS flex_game,
        salary AS flex_sal,
        fpts AS flex_fpts,
        value AS flex_val,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY fpts DESC) AS rank_in_team
    FROM NFLMAIN.PUBLIC.roster
    WHERE position LIKE '%RB%' 
    or position LIKE '%WR%'


