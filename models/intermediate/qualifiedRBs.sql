select *
from {{ ref("topRBs") }}
where rb_val > (select avg(rb_val) from {{ ref("topRBs") }})
