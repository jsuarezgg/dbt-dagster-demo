

select
pd1.client_id,
pd1.application_id,
pd1.ip_address,
pd1.application_date,
count(pd2.client_id) as repeats
from gold.fmt_bureau_check_co pd1
left join gold.fmt_bureau_check_co pd2 on replace(pd1.ip_address,'.','1')::numeric = replace(pd2.ip_address,'.','1')::numeric and pd1.client_id != pd2.client_id and pd1.application_date > pd2.application_date
where pd1.ip_address is not null
and pd2.ip_address is not null
group by 1,2,3,4