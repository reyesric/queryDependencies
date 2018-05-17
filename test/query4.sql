with base as (
    select *
    from dw.customers
)
select top 100 *
from BASE
    join Months
        on 1=0
;