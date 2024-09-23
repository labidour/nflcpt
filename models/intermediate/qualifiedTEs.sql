select *
from {{ ref("topTEs") }}
where te_val > (select avg(te_val) from {{ ref("topTEs") }})
