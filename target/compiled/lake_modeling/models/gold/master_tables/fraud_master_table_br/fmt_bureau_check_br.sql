

with prev_phones_emails as (
  select
  a.application_id,
  a.client_id,
  a.application_date,
  a.application_cellphone,
  lower(a.application_email) as application_email,
  coalesce(idv.ip_address, iov.iovation_data_details_realIp_address) as ip_address,
  sum(case when a.application_cellphone != a2.application_cellphone and a.application_cellphone is not null and a2.application_cellphone is not null then 1 end) as diff_past_cellphones,
  sum(case when lower(a.application_email) != lower(a2.application_email) and a.application_email is not null and a2.application_email is not null then 1 end) as diff_past_emails
  from silver.f_pii_applications_br a
  left join silver.f_pii_applications_br a2
    on a.client_id = a2.client_id 
    and a.application_date > a2.application_date
    and (a.application_cellphone != a2.application_cellphone or lower(a.application_email) != lower(a2.application_email))
  left join silver.f_idv_stage_br idv
    on a.application_id = idv.application_id
  left join silver.f_kyc_iovation_v1v2_br iov
    on a.application_id = iov.application_id
  group by 1,2,3,4,5,6
)
select 
a.*,
case when instr(b.cellphones_string, a.application_cellphone) > 0 then 1 else 0
     end as cellphone_match,
case when instr(lower(b.emails_string), lower(a.application_email)) > 0 then 1 else 0
     end as email_match
from prev_phones_emails a
left join gold.fmt_kyc_all_info_br b 
  on a.application_id = b.application_id