SELECT 
    -- Constructing the sorted combo in the specified order
    ARRAY_SORT(ARRAY_CONSTRUCT(
        rb_flex_dst.RB01_ID, rb_flex_dst.RB02_ID, 
        wr_te.WR01_ID, wr_te.WR02_ID, wr_te.WR03_ID, 
        wr_te.TE_ID, rb_flex_dst.FLEX_ID, rb_flex_dst.DST_ID
    )) AS sorted_combo,

    -- Selecting fields from the RB_Flex_DST source
    rb_flex_dst.RB01_ID, rb_flex_dst.RB01, rb_flex_dst.RB01_POS, rb_flex_dst.RB01_TEAM, rb_flex_dst.RB01_OPP, rb_flex_dst.RB01_GAME,
    rb_flex_dst.RB02_ID, rb_flex_dst.RB02, rb_flex_dst.RB02_POS, rb_flex_dst.RB02_TEAM, rb_flex_dst.RB02_OPP, rb_flex_dst.RB02_GAME,

    -- Selecting fields from the WR_TE source
    wr_te.WR01_ID, wr_te.WR01, wr_te.WR01_POS, wr_te.WR01_TEAM, wr_te.WR01_OPP, wr_te.WR01_GAME,
    wr_te.WR02_ID, wr_te.WR02, wr_te.WR02_POS, wr_te.WR02_TEAM, wr_te.WR02_OPP, wr_te.WR02_GAME,
    wr_te.WR03_ID, wr_te.WR03, wr_te.WR03_POS, wr_te.WR03_TEAM, wr_te.WR03_OPP, wr_te.WR03_GAME,

    -- Selecting TE, Flex, and DST fields
    wr_te.TE_ID, wr_te.TE, wr_te.TE_POS, wr_te.TE_TEAM, wr_te.TE_OPP, wr_te.TE_GAME,
    rb_flex_dst.FLEX_ID, rb_flex_dst.FLEX, rb_flex_dst.FLEX_POS, rb_flex_dst.FLEX_TEAM, rb_flex_dst.FLEX_OPP, rb_flex_dst.FLEX_GAME,
    rb_flex_dst.DST_ID, rb_flex_dst.DST, rb_flex_dst.DST_POS, rb_flex_dst.DST_TEAM, rb_flex_dst.DST_OPP, rb_flex_dst.DST_GAME,

    -- Summing up salary, fpts, and val from both sources
    (rb_flex_dst.SAL + wr_te.SAL) AS sal,
    (rb_flex_dst.FPTS + wr_te.FPTS) AS fpts,
    (rb_flex_dst.VAL + wr_te.VAL) AS val

FROM {{ ref("qualified_WR_TE") }} wr_te
JOIN {{ ref("qualified_RBs_Flex_DST") }} rb_flex_dst
    -- Ensure none of the teams are the same
    ON wr_te.WR01_TEAM != rb_flex_dst.RB01_TEAM
    AND wr_te.WR01_TEAM != rb_flex_dst.RB02_TEAM
    AND wr_te.WR01_TEAM != rb_flex_dst.FLEX_TEAM
    AND wr_te.WR02_TEAM != rb_flex_dst.RB01_TEAM
    AND wr_te.WR02_TEAM != rb_flex_dst.RB02_TEAM
    AND wr_te.WR02_TEAM != rb_flex_dst.FLEX_TEAM
    AND wr_te.WR03_TEAM != rb_flex_dst.RB01_TEAM
    AND wr_te.WR03_TEAM != rb_flex_dst.RB02_TEAM
    AND wr_te.WR03_TEAM != rb_flex_dst.FLEX_TEAM
    AND wr_te.TE_TEAM != rb_flex_dst.RB01_TEAM
    AND wr_te.TE_TEAM != rb_flex_dst.RB02_TEAM
    AND wr_te.TE_TEAM != rb_flex_dst.FLEX_TEAM

    -- Ensure the WRs' opponents and TE's opponent do not match the DST team
    AND wr_te.WR01_OPP != rb_flex_dst.DST_TEAM
    AND wr_te.WR02_OPP != rb_flex_dst.DST_TEAM
    AND wr_te.WR03_OPP != rb_flex_dst.DST_TEAM
    AND wr_te.TE_OPP != rb_flex_dst.DST_TEAM

WHERE (rb_flex_dst.SAL + wr_te.SAL) <= 45500
