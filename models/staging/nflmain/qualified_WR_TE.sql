WITH ranked_combos AS (
    SELECT
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

        raw.te_id AS te_id,
        raw.te AS te,
        raw.te_pos AS te_pos,
        raw.te_team AS te_team,
        raw.te_opp AS te_opp,
        raw.te_game AS te_game,

        raw.sal,
        raw.fpts,
        raw.val,

        raw.sorted_combo,

        -- Use ROW_NUMBER to assign a row number for each sorted_combo, ordered by SAL
        ROW_NUMBER() OVER (PARTITION BY raw.sorted_combo ORDER BY raw.sal ASC) AS row_num

    FROM {{ ref("combo_WR_TE_raw") }} raw
    JOIN {{ ref("combo_WR_TE_distinct") }} dist
        ON raw.sorted_combo = dist.sorted_combo
    WHERE raw.sal <= 32500
)

-- Select only the first row for each sorted_combo
SELECT
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

    te_id,
    te,
    te_pos,
    te_team,
    te_opp,
    te_game,

    sal,
    fpts,
    val,
    sorted_combo

FROM ranked_combos
WHERE row_num = 1