{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

{% set time_zone = 'America/Sao_Paulo' %}

with loan_data as (
  select 
  client_id,
  loan_id,
  approved_amount,
  from_utc_timestamp(origination_date, "{{time_zone}}") AS origination_date,
  state,
  first_value(from_utc_timestamp(origination_date, "{{time_zone}}")) over (partition by client_id order by origination_date) as first_origination_date,
  first_value(from_utc_timestamp(origination_date, "{{time_zone}}")) over (partition by client_id order by origination_date desc) as last_origination_date,
  lag(from_utc_timestamp(origination_date, "{{time_zone}}")) over (partition by client_id order by origination_date) as prev_origination_date,
  row_number() over (partition by client_id order by origination_date) as rn
  from {{ ref('dm_loan_status_br') }}
),
preap_apps as (
  select *,
  prospect_id as client_id
  from {{ ref('rmt_application_preapproval_br') }}
  where rn = 1
),
vertical AS (
	SELECT
		DISTINCT ally_slug,
		ally_vertical:name.value AS vertical
	FROM {{ ref('d_ally_management_stores_allies_br') }}
),
app_count as (
  select
  a.client_id,
  orig.loan_id,
  a.application_id,
  from_utc_timestamp(a.application_date, "{{time_zone}}") AS application_date,
  a.ally_slug as ally_name,
  a.requested_amount,
  v.vertical,
  ld.first_origination_date,
  case when from_utc_timestamp(a.application_date, "{{time_zone}}") < ld.first_origination_date or ld.first_origination_date is null then 'PROSPECT' else 'CLIENT' end as client_type
  from {{ ref('f_applications_br') }} a
  left join {{ ref('f_originations_bnpl_br') }} orig
      on a.application_id = orig.application_id
  left join {{ ref('rmt_application_product_br') }} ap 
    on a.application_id = ap.application_id 
  left join loan_data ld 
    on a.client_id = ld.client_id and ld.rn = 1
  LEFT JOIN vertical v
    ON a.ally_slug = v.ally_slug
  where ap.product = 'PAGO_BR'
),
raw_behaviour as (
  select 
  ac.client_id,
  max(pa.preapproval_amount) as preapp_amount,
  sum(case when ac.application_date - interval '7 days' < pa.application_date then ac.requested_amount end) as apps7days, 
  sum(case when ac.application_date - interval '7 days' < pa.application_date then ac.requested_amount end) / max(pa.preapproval_amount) as p_apps7days
  from app_count ac
  left join preap_apps pa 
    on ac.client_id = pa.client_id
  group by 1
),
raw_behaviour_loan as (
  select 
  ld.client_id,
  sum(case when ld.rn=2 and ld.origination_date - interval '7 days' < ld.first_origination_date then ac.requested_amount end) / sum(case when ld.rn=1 then ac.requested_amount end) as second_vs_first
  from loan_data ld
  left join app_count ac
    on ld.loan_id = ac.loan_id
  group by 1  
),
apps_7_days as (
  select
  t1.application_id,
  t1.loan_id,
  t1.client_id,
  count(t2.application_id) as weekly_apps,
  sum(case when t1.application_id is not null and t1.requested_amount > t2.requested_amount then 1 end) as lesser_weekly_apps,
  sum(case when t1.ally_name != t2.ally_name and t1.ally_name is not null and t2.ally_name is not null and t1.requested_amount != t2.requested_amount then 1 end) as diff_ally_n_amount_weekly,
  sum(case when t1.ally_name != t2.ally_name and t1.ally_name is not null and t2.ally_name is not null and t1.requested_amount < t2.requested_amount then 1 end) as diff_ally_less_amount_weekly,
  sum(case when t1.requested_amount < t2.requested_amount then 1 end) as less_amount_weekly,
  sum(case when t1.vertical != t2.vertical then 1 end) as diff_vertical_weekly
  from app_count t1
  left join app_count t2 
    on t1.client_id = t2.client_id 
    and t2.application_date BETWEEN t1.application_date - interval '7 days' and t1.application_date
    and t1.application_id != t2.application_id
  group by 
  t1.application_id,
  t1.loan_id,
  t1.client_id
),
apps_30_days as (
  select
  t1.application_id,
  t1.loan_id,
  t1.client_id,
  count(t2.application_id) as monthly_apps,
  sum(case when t1.application_id is not null and t1.requested_amount > t2.requested_amount then 1 end) as lesser_monthly_apps,
  sum(case when t1.ally_name != t2.ally_name and t1.ally_name is not null and t2.ally_name is not null and t1.requested_amount != t2.requested_amount then 1 end) as diff_ally_n_amount_monthly,
  sum(case when t1.ally_name != t2.ally_name and t1.ally_name is not null and t2.ally_name is not null and t1.requested_amount < t2.requested_amount then 1 end) as diff_ally_less_amount_monthly,
  sum(case when t1.requested_amount < t2.requested_amount then 1 end) as less_amount_monthly,
  sum(case when t1.vertical != t2.vertical then 1 end) as diff_vertical_monthly
  from app_count t1
  left join app_count t2 
    on t1.client_id = t2.client_id 
    and t2.application_date BETWEEN t1.application_date - interval '30 days' and t1.application_date
    and t1.application_id != t2.application_id
  group by 
  t1.application_id,
  t1.loan_id,
  t1.client_id
)
select 
apps_7_days.application_id,
apps_7_days.loan_id,
apps_7_days.client_id,
apps_7_days.weekly_apps,
apps_7_days.lesser_weekly_apps,
apps_7_days.diff_ally_n_amount_weekly,
apps_7_days.diff_ally_less_amount_weekly,
apps_7_days.less_amount_weekly,
apps_7_days.diff_vertical_weekly,
apps_30_days.monthly_apps,
apps_30_days.lesser_monthly_apps,
apps_30_days.diff_ally_n_amount_monthly,
apps_30_days.diff_ally_less_amount_monthly,
apps_30_days.less_amount_monthly,
apps_30_days.diff_vertical_monthly,
raw_behaviour.apps7days,
raw_behaviour.p_apps7days,
raw_behaviour_loan.second_vs_first,
raw_behaviour.preapp_amount
from apps_7_days 
left join apps_30_days
  on apps_7_days.application_id = apps_30_days.application_id
left join raw_behaviour
  on apps_7_days.client_id = raw_behaviour.client_id
left join raw_behaviour_loan
  on apps_7_days.client_id = raw_behaviour_loan.client_id  

/*
  MAIN CHANGES:
  1. variable STATE was removed from filters
  2. diff_ally_n_amount -> diff_ally_n_amount_weekly
     diff_ally_less_amount -> diff_ally_less_amount_weekly
     less_amount -> less_amount_weekly
     diff_vertical -> diff_vertical_weekly
     
     
  3. Added variables monthly basis:
      monthly_apps
      lesser_monthly_apps
      diff_ally_n_amount_monthly
      diff_ally_less_amount_monthly
      less_amount_monthly
      diff_vertical_monthly
*/

--BR cambiar approved amount por RA 
/*1. linea 24, cambiar por application date DONE
1.1 cambiar loans data por applications DONE
1.2 Rename labels orig7days DONE
2. orig7days & p_orig7days a applications, dejar 2nd vs 1st como originations DONE
3. linea 53, excluir BNPN DONE
4. linea 71 eliminar where t1.loan_id is not null 
  and t1.client_type = 'PROSPECT'
5.  linea 94 eliminar where t1.loan_id is not null
  and t1.client_type = 'PROSPECT'
6. Cambiar joins   
  */
