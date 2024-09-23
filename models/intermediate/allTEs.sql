-- Rank TEs by fpts per team
    SELECT
        id AS te_id,
        name AS te,
        'TE' AS te_pos,
        team AS te_team,
        opp AS te_opp,
        game AS te_game,
        salary AS te_sal,
        fpts AS te_fpts,
        value AS te_val,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY fpts DESC) AS rank_in_team
    FROM NFLMAIN.PUBLIC.roster
    WHERE position LIKE '%TE%'


