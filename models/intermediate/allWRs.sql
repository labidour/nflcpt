-- Rank WRs by fpts per team
    SELECT
        id AS wr_id,
        name AS wr,
        'WR' AS wr_pos,
        team AS wr_team,
        opp AS wr_opp,
        game AS wr_game,
        salary AS wr_sal,
        fpts AS wr_fpts,
        value AS wr_val,
        ROW_NUMBER() OVER (PARTITION BY team ORDER BY fpts DESC) AS rank_in_team
    FROM NFLMAIN.PUBLIC.roster
    WHERE position LIKE '%WR%'


