SELECT
    qb.qb,  -- Quarterback
    ros.rb01,  -- Running back 1
    ros.rb02,  -- Running back 2
    ros.wr01,  -- Wide receiver 1
    ros.wr02,  -- Wide receiver 2
    ros.wr03,  -- Wide receiver 3
    ros.te,    -- Tight end
    ros.flex,  -- Flex player
    ros.dst,   -- Defense/Special teams

    -- Summing the salary, fpts, and value across all positions
    (qb.qb_sal + ros.sal) AS sal,
    (qb.qb_fpts + ros.fpts) AS fpts,
    (qb.qb_val + ros.val) AS val

FROM {{ ref("qualifiedQBs") }} qb
JOIN {{ ref("qualified_RB_WR_TE_FLEX_DST") }} ros
    -- Match QB team with WR01, WR02, WR03, or TE team
    ON qb.qb_team IN (ros.wr01_team, ros.wr02_team, ros.wr03_team, ros.te_team)

-- Only include lineups where the total salary is <= 50000
WHERE (qb.qb_sal + ros.sal) <= 50000
