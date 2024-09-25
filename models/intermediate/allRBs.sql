-- Rank RBs by fpts per team
    SELECT
        id AS rb_id,
        name AS rb,
        'RB' AS rb_pos,
        team AS rb_team,
        opp AS rb_opp,
        game AS rb_game,
        salary AS rb_sal,
        fpts AS rb_fpts,
        value AS rb_val,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY fpts DESC) AS rank_in_team
    FROM NFLMAIN.PUBLIC.roster
    WHERE position LIKE '%RB%'


