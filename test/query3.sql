select *
from dw.months 
    join (
        select * from dbo.customers where 1=1)