SELECT 
    ARRAY_SORT(ARRAY_CONSTRUCT(wr01.wr_id, wr02.wr_id, wr03.wr_id, te.te_id)) AS sorted_combo,    

    wr01.wr_id AS wr01_id,
    wr01.wr AS wr01,
    wr01.wr_pos AS wr01_pos,
    wr01.wr_team AS wr01_team,
    wr01.wr_opp AS wr01_opp,
    wr01.wr_game AS wr01_game,

    wr02.wr_id AS wr02_id,
    wr02.wr AS wr02,
    wr02.wr_pos AS wr02_pos,
    wr02.wr_team AS wr02_team,
    wr02.wr_opp AS wr02_opp,
    wr02.wr_game AS wr02_game,

    wr03.wr_id AS wr03_id,
    wr03.wr AS wr03,
    wr03.wr_pos AS wr03_pos,
    wr03.wr_team AS wr03_team,
    wr03.wr_opp AS wr03_opp,
    wr03.wr_game AS wr03_game,

    te.te_id,
    te.te,
    te.te_pos,
    te.te_team,
    te.te_opp,
    te.te_game,

    wr01.wr_sal + wr02.wr_sal + wr03.wr_sal + te.te_sal AS sal,
    wr01.wr_fpts + wr02.wr_fpts + wr03.wr_fpts + te.te_fpts AS fpts,
    wr01.wr_val + wr02.wr_val + wr03.wr_val + te.te_val AS val
    
FROM {{ ref("qualifiedWRs") }} wr01
JOIN {{ ref("qualifiedWRs") }} wr02
    ON wr01.wr_team != wr02.wr_team

JOIN {{ ref("qualifiedWRs") }} wr03
    ON wr01.wr_team != wr03.wr_team
    AND wr02.wr_team != wr03.wr_team

JOIN {{ ref("qualifiedTEs") }} te
    ON wr01.wr_opp != te.te_team
    AND wr02.wr_opp != te.te_team
    AND wr03.wr_opp != te.te_team