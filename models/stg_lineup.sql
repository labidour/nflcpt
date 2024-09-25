-- This model selects from the NFL lineups data
select
    QB,
    RB01,
    RB02,
    WR01,
    WR02,
    WR03,
    TE,
    FLEX,
    DST,
    SAL,
    FPTS,
    VAL
from {{ source('nflmain', 'lineups') }}
