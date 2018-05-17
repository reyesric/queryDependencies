update t
set name=''
from customers t
    join months m
        on t.monthKey = m.monthKey;