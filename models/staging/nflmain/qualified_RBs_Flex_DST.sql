WITH ranked_combos AS (
    SELECT
        raw.RB01_ID,
        raw.RB01,
        raw.RB01_POS,
        raw.RB01_TEAM,
        raw.RB01_OPP,
        raw.RB01_GAME,

        raw.RB02_ID,
        raw.RB02,
        raw.RB02_POS,
        raw.RB02_TEAM,
        raw.RB02_OPP,
        raw.RB02_GAME,

        raw.FLEX_ID,
        raw.FLEX,
        raw.FLEX_POS,
        raw.FLEX_TEAM,
        raw.FLEX_OPP,
        raw.FLEX_GAME,

        raw.DST_ID,
        raw.DST,
        raw.DST_POS,
        raw.DST_TEAM,
        raw.DST_OPP,
        raw.DST_GAME,

        raw.SAL,
        raw.FPTS,
        raw.VAL,

        raw.sorted_combo,

        -- Use ROW_NUMBER to assign a row number for each sorted_combo, ordered by SAL
        ROW_NUMBER() OVER (PARTITION BY raw.sorted_combo ORDER BY raw.SAL ASC) AS row_num

    FROM {{ ref("combo_RBs_Flex_DST_raw") }} raw
    JOIN {{ ref("combo_RBs_Flex_DST_distinct") }} dist
        ON raw.sorted_combo = dist.sorted_combo
    WHERE raw.SAL <= 34000
)

-- Select only the first row for each sorted_combo
SELECT
    RB01_ID,
    RB01,
    RB01_POS,
    RB01_TEAM,
    RB01_OPP,
    RB01_GAME,

    RB02_ID,
    RB02,
    RB02_POS,
    RB02_TEAM,
    RB02_OPP,
    RB02_GAME,

    FLEX_ID,
    FLEX,
    FLEX_POS,
    FLEX_TEAM,
    FLEX_OPP,
    FLEX_GAME,

    DST_ID,
    DST,
    DST_POS,
    DST_TEAM,
    DST_OPP,
    DST_GAME,

    SAL,
    FPTS,
    VAL,
    sorted_combo

FROM ranked_combos
WHERE row_num = 1