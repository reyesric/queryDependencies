select *
from dw.Months mo
    join fact.sales s
        on s.monthKey = s.monthKey
;
