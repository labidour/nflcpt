SELECT
    dk_id AS id,
    dk_name_id AS name,
    dk_roster_position AS position,
    rg_team AS team,
    rg_opp AS opp,
    dk_game_info AS game,
    dk_salary AS salary,
    COALESCE(dk_avgfpts, 0) AS avgfpts,
    COALESCE(rw_fpts, 0) AS rwfpts,
    COALESCE(rg_fpts, 0) AS rgfpts,

    -- Conditional logic for applying the 0.95 multiplier only when appropriate
    CASE
        -- If RW is higher than RG, apply 0.95 multiplier to RW unless it drops it below RG
        WHEN COALESCE(rw_fpts, 0) > COALESCE(rg_fpts, 0) 
            AND 0.95 * COALESCE(rw_fpts, 0) > COALESCE(rg_fpts, 0) 
        THEN (0.95 * COALESCE(rw_fpts, 0) + COALESCE(rg_fpts, 0)) / 2
        
        -- If RG is higher than RW, apply 0.95 multiplier to RG unless it drops it below RW
        WHEN COALESCE(rg_fpts, 0) > COALESCE(rw_fpts, 0) 
            AND 0.95 * COALESCE(rg_fpts, 0) > COALESCE(rw_fpts, 0) 
        THEN (COALESCE(rw_fpts, 0) + 0.95 * COALESCE(rg_fpts, 0)) / 2

        -- If no adjustments are needed, just average the two
        ELSE (COALESCE(rw_fpts, 0) + COALESCE(rg_fpts, 0)) / 2
    END AS fpts,

    -- fpts_per_1000 based on the final fpts calculation
    ((CASE
        WHEN COALESCE(rw_fpts, 0) > COALESCE(rg_fpts, 0) 
            AND 0.95 * COALESCE(rw_fpts, 0) > COALESCE(rg_fpts, 0) 
        THEN (0.95 * COALESCE(rw_fpts, 0) + COALESCE(rg_fpts, 0)) / 2
        
        WHEN COALESCE(rg_fpts, 0) > COALESCE(rw_fpts, 0) 
            AND 0.95 * COALESCE(rg_fpts, 0) > COALESCE(rw_fpts, 0) 
        THEN (COALESCE(rw_fpts, 0) + 0.95 * COALESCE(rg_fpts, 0)) / 2

        ELSE (COALESCE(rw_fpts, 0) + COALESCE(rg_fpts, 0)) / 2
    END) / dk_salary) * 1000 AS value

FROM NFLMAIN.PUBLIC.PLAYERS p
JOIN NFLMAIN.PUBLIC.DRAFTKINGS dk
    ON p.draftkings = dk_name
JOIN NFLMAIN.PUBLIC.ROTOWIRE rw
    ON p.rotowire = rw_player
JOIN NFLMAIN.PUBLIC.ROTOGRINDERS rg
    ON p.rotogrinder = rg_name
