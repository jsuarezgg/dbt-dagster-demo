

select
pd1.client_id,
pd1.application_id,
pd1.application_email,
pd1.application_date,
count(pd2.client_id) as repeats
from gold.fmt_bureau_check_br pd1
left join gold.fmt_bureau_check_br pd2 on lower(pd1.application_email) = lower(pd2.application_email) and pd1.client_id != pd2.client_id and pd1.application_date > pd2.application_date
where pd1.application_email is not null
and pd2.application_email is not null
group by 1,2,3,4