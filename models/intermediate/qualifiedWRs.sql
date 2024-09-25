select *
from {{ref("topWRs")}}
where wr_val > (select avg(wr_val)
from {{ref("topWRs")}})