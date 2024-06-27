

with past_x_applications as (
  select 
  a.client_id,
  a.application_id,
  a.requested_amount,
  a.application_date,
  unix_timestamp(a.application_date) application_date_unix,
  row_number() over (partition by a.client_id order by a.application_date desc) app_order
  from silver.f_applications_br a
  left join gold.rmt_application_product_br ap 
    on a.application_id = ap.application_id
  where a.application_date >= add_months(current_date(),-18)
  and lower(ap.product) like '%pago_br%'
),          
times as (        
  select *,
  (application_date_unix - lead(application_date_unix) over(partition by client_id order by app_order))/60 dif_2_application_min,
  (application_date_unix - lead(application_date_unix,2) over(partition by client_id order by app_order))/60 dif_3_application_min,
  lead(requested_amount) over (partition by client_id order by app_order asc) past_2_req_amt,
  lead(requested_amount,2) over (partition by client_id order by app_order asc) past_3_req_amt
  from past_x_applications
), 
tagged as (
  select client_id,
  application_id,
  application_date,
  app_order,
  case when dif_2_application_min <= 720 and requested_amount < past_2_req_amt and dif_3_application_min <= 720 and past_2_req_amt < past_3_req_amt then 1 else 0 
  end as tag_walkdown            
  from times
)
select 
client_id, 
application_id, 
max(tag_walkdown) mr_walkdown
from tagged
group by 1,2