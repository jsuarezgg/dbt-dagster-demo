
    
    

select
    payment_id as unique_field,
    count(*) as n_records

from silver.f_fincore_payments_co
where payment_id is not null
group by payment_id
having count(*) > 1


