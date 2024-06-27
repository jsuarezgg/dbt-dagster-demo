

-- QUERY FOR BRASIL

with pago_fpd_br as (
	select
	  'BR' as country_code,
	  to_date(date_trunc('DAY',fp_date_plus_1_month)) as dpd31_date,
	  a.client_type,
	  sum(a.UPB_plus_15_day) as upb_dpd_plus15,
	  sum(a.UPB_plus_1_month) as upb_dpd_plus31, 
	  sum(a.general_amount) as opb,
	  sum(case when a.DPD_plus_15_day>=1 then a.UPB_plus_15_day when a.DPD_plus_15_day<1 then 0 else null end) as dq1_at_15_upb, 
	  sum(case when a.dpd_plus_1_month>1 and CAST(l.total_interest AS NUMERIC) = 0  then a.UPB_plus_1_month when a.dpd_plus_1_month<=1 then 0 else null end) dq31_at_31_upb,
      sum(case when a.dpd_plus_1_month>1 and CAST(l.total_interest AS NUMERIC) > 0 then a.UPB_plus_1_month when a.dpd_plus_1_month<=1 then 0 else null end) dq31_at_31_upb_flex,
	  sum(case when a.DPD_plus_15_day is not null then a.general_amount end) dq1_at_15_opb,
	  sum(case when a.dpd_plus_1_month is not null and CAST(l.total_interest AS NUMERIC) = 0  then a.general_amount end) dq31_at_31_opb,
      sum(case when a.dpd_plus_1_month is not null and CAST(l.total_interest AS NUMERIC) > 0 then a.general_amount end) dq31_at_31_opb_flex
	from gold.risk_master_table_br a
	LEFT JOIN silver.f_loan_proposals_br l
		ON a.loan_id = l.loan_proposal_id and a.loan_id is not null
	where a.product like '%PAGO%'
	  --and a.client_type='prospect'
	  --and fp_date_plus_1_month >= '2021-01-01'
	  and fp_date_plus_1_month >= date_trunc('month',current_date()) - interval '13 month'
	  and a.dpd_plus_1_month is not null
	  and a.loan_id is not null
	group by 1,2,3
	order by 2 desc
),

--QUERY FOR COLOMBIA

pago_fpd_co as (
	select
	  'CO' as country_code,
	  to_date(date_trunc('DAY',fp_date_plus_1_month)) as dpd31_date,
	  a.client_type as client_type,
	  sum(a.UPB_plus_15_day) as upb_dpd_plus15,
	  sum(a.UPB_plus_1_month) as upb_dpd_plus31,
	  sum(a.approved_amount) as opb,
	  sum(case when a.DPD_plus_15_day>=1 then a.UPB_plus_15_day when a.DPD_plus_15_day<1 then 0 else null end) as dq1_at_15_upb,
	  sum(case when a.dpd_plus_1_month>1 and CAST(l.total_interest AS NUMERIC) = 0 then a.UPB_plus_1_month when a.dpd_plus_1_month<=1 then 0 else null end) dq31_at_31_upb,
      sum(case when a.dpd_plus_1_month>1 and CAST(l.total_interest AS NUMERIC) > 0 then a.UPB_plus_1_month when a.dpd_plus_1_month<=1 then 0 else null end) dq31_at_31_upb_flex,
	  sum(case when a.DPD_plus_15_day is not null then a.approved_amount end) dq1_at_15_opb,
	  sum(case when a.dpd_plus_1_month is not null and CAST(l.total_interest AS NUMERIC) = 0 then a.approved_amount end) dq31_at_31_opb,
      sum(case when a.dpd_plus_1_month is not null and CAST(l.total_interest AS NUMERIC) > 0 then a.approved_amount end) dq31_at_31_opb_flex
	from gold.risk_master_table_co a
	LEFT JOIN silver.f_loan_proposals_co l
		ON a.loan_id = l.loan_proposal_id and a.loan_id is not null
	where a.product like '%PAGO%'
	  --and a.client_type='prospect'
	  --and fp_date_plus_1_month >= '2021-01-01'
	  and fp_date_plus_1_month >= date_trunc('month',current_date()) - interval '13 month'
	  and a.dpd_plus_1_month is not null
	  and a.loan_id is not null
	group by 1,2,3
	order by 2 desc
),

unified_fpd as (
	select * from pago_fpd_br
	union all
	select * from pago_fpd_co
)

select *
from unified_fpd