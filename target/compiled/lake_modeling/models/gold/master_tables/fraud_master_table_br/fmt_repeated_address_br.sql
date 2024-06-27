

with apps_data as (
  select a.*,
  orig.origination_date,
  b.client_id
  from gold.fmt_clean_address_apps_br a
  left join silver.f_applications_br b on a.application_id = b.application_id
  left join silver.f_originations_bnpl_br orig on a.application_id = orig.application_id
)
select
a.application_id, 
max(case when a.client_id != b.client_id then 1 else 0 end) repeated_address
from apps_data a 
left join apps_data b on upper(b.fixed_address) = upper(a.fixed_address) and b.origination_date < a.origination_date
group by a.application_id