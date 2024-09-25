select *
from {{ ref("topFLEXs") }}
where flex_val > (select avg(flex_val) from {{ ref("topFLEXs") }})
