
    
    

select
    loan_id as unique_field,
    count(*) as n_records

from silver.f_originations_bnpl_br
where loan_id is not null
group by loan_id
having count(*) > 1


