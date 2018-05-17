delete from customers c
where c.customerId in (
    select id from oldcustomers)