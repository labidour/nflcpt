WITH ranked_combos AS (
    SELECT
        -- Selecting RBs
        raw.rb01_id AS rb01_id,
        raw.rb01 AS rb01,
        raw.rb01_pos AS rb01_pos,
        raw.rb01_team AS rb01_team,
        raw.rb01_opp AS rb01_opp,
        raw.rb01_game AS rb01_game,

        raw.rb02_id AS rb02_id,
        raw.rb02 AS rb02,
        raw.rb02_pos AS rb02_pos,
        raw.rb02_team AS rb02_team,
        raw.rb02_opp AS rb02_opp,
        raw.rb02_game AS rb02_game,

        -- Selecting WRs
        raw.wr01_id AS wr01_id,
        raw.wr01 AS wr01,
        raw.wr01_pos AS wr01_pos,
        raw.wr01_team AS wr01_team,
        raw.wr01_opp AS wr01_opp,
        raw.wr01_game AS wr01_game,

        raw.wr02_id AS wr02_id,
        raw.wr02 AS wr02,
        raw.wr02_pos AS wr02_pos,
        raw.wr02_team AS wr02_team,
        raw.wr02_opp AS wr02_opp,
        raw.wr02_game AS wr02_game,

        raw.wr03_id AS wr03_id,
        raw.wr03 AS wr03,
        raw.wr03_pos AS wr03_pos,
        raw.wr03_team AS wr03_team,
        raw.wr03_opp AS wr03_opp,
        raw.wr03_game AS wr03_game,

        -- Selecting TE
        raw.te_id AS te_id,
        raw.te AS te,
        raw.te_pos AS te_pos,
        raw.te_team AS te_team,
        raw.te_opp AS te_opp,
        raw.te_game AS te_game,

        -- Selecting Flex
        raw.flex_id AS flex_id,
        raw.flex AS flex,
        raw.flex_pos AS flex_pos,
        raw.flex_team AS flex_team,
        raw.flex_opp AS flex_opp,
        raw.flex_game AS flex_game,

        -- Selecting DST
        raw.dst_id AS dst_id,
        raw.dst AS dst,
        raw.dst_pos AS dst_pos,
        raw.dst_team AS dst_team,
        raw.dst_opp AS dst_opp,
        raw.dst_game AS dst_game,

        -- Summing salary, fantasy points, and value
        raw.sal,
        raw.fpts,
        raw.val,

        raw.sorted_combo,

        -- Use ROW_NUMBER to assign a row number for each sorted_combo, ordered by SAL
        ROW_NUMBER() OVER (PARTITION BY raw.sorted_combo ORDER BY raw.sal ASC) AS row_num

    FROM {{ ref("combo_RB_WR_TE_FLEX_DST_raw") }} raw
    JOIN {{ ref("combo_RB_WR_TE_FLEX_DST_distinct") }} dist
        ON raw.sorted_combo = dist.sorted_combo
)

-- Select only the first row for each sorted_combo
SELECT
    -- Selecting RBs
    rb01_id,
    rb01,
    rb01_pos,
    rb01_team,
    rb01_opp,
    rb01_game,

    rb02_id,
    rb02,
    rb02_pos,
    rb02_team,
    rb02_opp,
    rb02_game,

    -- Selecting WRs
    wr01_id,
    wr01,
    wr01_pos,
    wr01_team,
    wr01_opp,
    wr01_game,

    wr02_id,
    wr02,
    wr02_pos,
    wr02_team,
    wr02_opp,
    wr02_game,

    wr03_id,
    wr03,
    wr03_pos,
    wr03_team,
    wr03_opp,
    wr03_game,

    -- Selecting TE
    te_id,
    te,
    te_pos,
    te_team,
    te_opp,
    te_game,

    -- Selecting Flex
    flex_id,
    flex,
    flex_pos,
    flex_team,
    flex_opp,
    flex_game,

    -- Selecting DST
    dst_id,
    dst,
    dst_pos,
    dst_team,
    dst_opp,
    dst_game,

    -- Total salary, fantasy points, and value
    sal,
    fpts,
    val,
    sorted_combo

FROM ranked_combos
WHERE row_num = 1