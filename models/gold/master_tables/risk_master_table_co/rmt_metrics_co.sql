{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

with initial_table as (
  select orig.loan_id,
         from_utc_timestamp(ls.first_payment_date, 'America/Bogota') as first_payment_date
  from {{ ref('f_applications_co') }} apps
  inner join (
    SELECT application_id, loan_id FROM {{ ref('f_originations_bnpl_co') }}
    UNION ALL
    SELECT application_id, loan_id FROM {{ ref('f_refinance_loans_co') }}
    )
    orig on apps.application_id = orig.application_id
  left join {{ ref('dm_loan_status_co') }} ls
    on orig.loan_id = ls.loan_id
  where orig.loan_id is not null
)

, payment_dates as (
    select 
      loan_id
      ,(first_payment_date + interval '1 day')::date  as FP_date_plus_1_day
      ,(first_payment_date + interval '5 day')::date as FP_date_plus_5_day
      ,(first_payment_date + interval '10 day')::date as FP_date_plus_10_day
      ,(first_payment_date + interval '15 day')::date as FP_date_plus_15_day
      ,(first_payment_date + interval '1 month'+ interval '1 day')::date as FP_date_plus_1_month
      ,(first_payment_date + interval '2 month'+ interval '1 day')::date as FP_date_plus_2_month
      ,(first_payment_date + interval '3 month'+ interval '1 day')::date as FP_date_plus_3_month
      ,(first_payment_date + interval '4 month'+ interval '1 day')::date as FP_date_plus_4_month
      ,(first_payment_date + interval '5 month'+ interval '1 day')::date as FP_date_plus_5_month
      ,(first_payment_date + interval '6 month'+ interval '1 day')::date as FP_date_plus_6_month
      ,(first_payment_date + interval '7 month' + interval '1 day')::date as FP_date_plus_7_month
      ,(first_payment_date + interval '8 month' + interval '1 day')::date as FP_date_plus_8_month
      ,(first_payment_date + interval '9 month' + interval '1 day')::date as FP_date_plus_9_month
      ,(first_payment_date + interval '10 month' + interval '1 day')::date as FP_date_plus_10_month
      ,(first_payment_date + interval '11 month' + interval '1 day')::date as FP_date_plus_11_month
      ,(first_payment_date + interval '12 month' + interval '1 day')::date as FP_date_plus_12_month
      ,(first_payment_date + interval '13 month' + interval '1 day')::date as FP_date_plus_13_month
      ,(first_payment_date + interval '14 month' + interval '1 day')::date as FP_date_plus_14_month
      ,(first_payment_date + interval '15 month' + interval '1 day')::date as FP_date_plus_15_month
      ,(first_payment_date + interval '16 month' + interval '1 day')::date as FP_date_plus_16_month
      ,(first_payment_date + interval '17 month' + interval '1 day')::date as FP_date_plus_17_month --MOB18 for GS
      ,(first_payment_date + interval '18 month' + interval '1 day')::date as FP_date_plus_18_month
      ,(first_payment_date + interval '19 month' + interval '1 day')::date as FP_date_plus_19_month
      ,(first_payment_date + interval '20 month' + interval '1 day')::date as FP_date_plus_20_month
      ,(first_payment_date + interval '21 month' + interval '1 day')::date as FP_date_plus_21_month
      ,(first_payment_date + interval '22 month' + interval '1 day')::date as FP_date_plus_22_month
      ,(first_payment_date + interval '23 month' + interval '1 day')::date as FP_date_plus_23_month
      ,(first_payment_date + interval '24 month' + interval '1 day')::date as FP_date_plus_24_month

    from initial_table
)
-- Calculate DPD in different timeframes
, dpd_w_dates as (
    select
      pd.loan_id
      ,max(case when dls.calculation_date = pd.FP_date_plus_1_day then least(ceiling(dls.days_past_due/30),12) end)  as DPD_plus_1_day
      ,max(case when dls.calculation_date = pd.FP_date_plus_5_day then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_5_day
      ,max(case when dls.calculation_date = pd.FP_date_plus_10_day then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_10_day
      ,max(case when dls.calculation_date = pd.FP_date_plus_15_day then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_15_day
      ,max(case when dls.calculation_date = pd.FP_date_plus_1_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_1_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_2_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_2_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_3_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_3_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_4_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_4_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_5_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_5_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_6_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_6_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_7_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_7_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_8_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_8_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_9_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_9_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_10_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_10_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_11_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_11_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_12_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_12_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_13_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_13_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_14_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_14_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_15_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_15_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_16_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_16_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_17_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_17_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_18_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_18_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_19_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_19_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_20_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_20_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_21_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_21_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_22_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_22_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_23_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_23_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_24_month then least(ceiling(dls.days_past_due/30),12) end) as DPD_plus_24_month
      --UPB
      ,max(case when dls.calculation_date = pd.FP_date_plus_1_day then dls.unpaid_principal else null end) as UPB_plus_1_day
      ,max(case when dls.calculation_date = pd.FP_date_plus_5_day then dls.unpaid_principal else null end) as UPB_plus_5_day
      ,max(case when dls.calculation_date = pd.FP_date_plus_10_day then dls.unpaid_principal else null end) as UPB_plus_10_day
      ,max(case when dls.calculation_date = pd.FP_date_plus_15_day then dls.unpaid_principal else null end) as UPB_plus_15_day
      ,max(case when dls.calculation_date = pd.FP_date_plus_1_month then dls.unpaid_principal else null end) as UPB_plus_1_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_2_month then dls.unpaid_principal else null end) as UPB_plus_2_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_3_month then dls.unpaid_principal else null end) as UPB_plus_3_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_4_month then dls.unpaid_principal else null end) as UPB_plus_4_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_5_month then dls.unpaid_principal else null end) as UPB_plus_5_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_6_month then dls.unpaid_principal else null end) as UPB_plus_6_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_7_month then dls.unpaid_principal else null end) as UPB_plus_7_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_8_month then dls.unpaid_principal else null end) as UPB_plus_8_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_9_month then dls.unpaid_principal else null end) as UPB_plus_9_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_10_month then dls.unpaid_principal else null end) as UPB_plus_10_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_11_month then dls.unpaid_principal else null end) as UPB_plus_11_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_12_month then dls.unpaid_principal else null end) as UPB_plus_12_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_13_month then dls.unpaid_principal else null end) as UPB_plus_13_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_14_month then dls.unpaid_principal else null end) as UPB_plus_14_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_15_month then dls.unpaid_principal else null end) as UPB_plus_15_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_16_month then dls.unpaid_principal else null end) as UPB_plus_16_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_17_month then dls.unpaid_principal else null end) as UPB_plus_17_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_18_month then dls.unpaid_principal else null end) as UPB_plus_18_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_19_month then dls.unpaid_principal else null end) as UPB_plus_19_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_20_month then dls.unpaid_principal else null end) as UPB_plus_20_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_21_month then dls.unpaid_principal else null end) as UPB_plus_21_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_22_month then dls.unpaid_principal else null end) as UPB_plus_22_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_23_month then dls.unpaid_principal else null end) as UPB_plus_23_month
      ,max(case when dls.calculation_date = pd.FP_date_plus_24_month then dls.unpaid_principal else null end) as UPB_plus_24_month
  from payment_dates as pd
  left join {{ source('gold', 'dm_daily_loan_status_co') }} as dls
  on pd.loan_id = dls.loan_id
  group by 1
)
--Add Condonations
--Identify loans that have condonations (>10k COP) as of today
--Deduce condonation date from daily_loan_status by getting first calculation_date with full condoned amount -> from gold.condonation_date
,condonation_by_mob as (
  select
    cd.loan_id
    ,case when pd.FP_date_plus_1_day <= cd.condonation_date then cd.condoned_amount end as condoned_amount_plus_1_day
    ,case when pd.FP_date_plus_5_day <= cd.condonation_date then cd.condoned_amount end as condoned_amount_plus_5_day
    ,case when pd.FP_date_plus_10_day <= cd.condonation_date then cd.condoned_amount end as condoned_amount_plus_10_day
    ,case when pd.FP_date_plus_15_day <= cd.condonation_date then cd.condoned_amount end as condoned_amount_plus_15_day
    ,case when pd.FP_date_plus_1_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_1_month
    ,case when pd.FP_date_plus_2_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_2_month
    ,case when pd.FP_date_plus_3_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_3_month
    ,case when pd.FP_date_plus_4_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_4_month
    ,case when pd.FP_date_plus_5_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_5_month
    ,case when pd.FP_date_plus_6_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_6_month
    ,case when pd.FP_date_plus_7_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_7_month
    ,case when pd.FP_date_plus_8_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_8_month
    ,case when pd.FP_date_plus_9_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_9_month
    ,case when pd.FP_date_plus_10_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_10_month
    ,case when pd.FP_date_plus_11_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_11_month
    ,case when pd.FP_date_plus_12_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_12_month
    ,case when pd.FP_date_plus_13_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_13_month
    ,case when pd.FP_date_plus_14_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_14_month
    ,case when pd.FP_date_plus_15_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_15_month
    ,case when pd.FP_date_plus_16_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_16_month
    ,case when pd.FP_date_plus_17_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_17_month
    ,case when pd.FP_date_plus_18_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_18_month
    ,case when pd.FP_date_plus_19_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_19_month
    ,case when pd.FP_date_plus_20_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_20_month
    ,case when pd.FP_date_plus_21_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_21_month
    ,case when pd.FP_date_plus_22_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_22_month
    ,case when pd.FP_date_plus_23_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_23_month
    ,case when pd.FP_date_plus_24_month <= cd.condonation_date then cd.condoned_amount end as condoned_amount_24_month
  from {{ ref('rmt_condonation_date_co') }} cd
  left join payment_dates pd on cd.loan_id = pd.loan_id
)
select pd.loan_id
      ,pd.FP_date_plus_1_day
      ,pd.FP_date_plus_5_day
      ,pd.FP_date_plus_10_day
      ,pd.FP_date_plus_15_day
      ,pd.FP_date_plus_1_month
      ,pd.FP_date_plus_2_month
      ,pd.FP_date_plus_3_month
      ,pd.FP_date_plus_4_month
      ,pd.FP_date_plus_5_month
      ,pd.FP_date_plus_6_month
      ,pd.FP_date_plus_7_month
      ,pd.FP_date_plus_8_month
      ,pd.FP_date_plus_9_month
      ,pd.FP_date_plus_10_month
      ,pd.FP_date_plus_11_month
      ,pd.FP_date_plus_12_month
      ,pd.FP_date_plus_13_month
      ,pd.FP_date_plus_14_month
      ,pd.FP_date_plus_15_month
      ,pd.FP_date_plus_16_month
      ,pd.FP_date_plus_17_month
      ,pd.FP_date_plus_18_month
      ,pd.FP_date_plus_19_month
      ,pd.FP_date_plus_20_month
      ,pd.FP_date_plus_21_month
      ,pd.FP_date_plus_22_month
      ,pd.FP_date_plus_23_month
      ,pd.FP_date_plus_24_month
      --Number of loans delinquent at each maturity
      ,dpd.DPD_plus_1_day
      ,dpd.DPD_plus_5_day
      ,dpd.DPD_plus_10_day
      ,dpd.DPD_plus_15_day
      ,dpd.DPD_plus_1_month
      ,dpd.DPD_plus_2_month
      ,dpd.DPD_plus_3_month
      ,dpd.DPD_plus_4_month
      ,dpd.DPD_plus_5_month
      ,dpd.DPD_plus_6_month
      ,dpd.DPD_plus_7_month
      ,dpd.DPD_plus_8_month
      ,dpd.DPD_plus_9_month
      ,dpd.DPD_plus_10_month
      ,dpd.DPD_plus_11_month
      ,dpd.DPD_plus_12_month
      ,dpd.DPD_plus_13_month
      ,dpd.DPD_plus_14_month
      ,dpd.DPD_plus_15_month
      ,dpd.DPD_plus_16_month
      ,dpd.DPD_plus_17_month
      ,dpd.DPD_plus_18_month
      ,dpd.DPD_plus_19_month
      ,dpd.DPD_plus_20_month
      ,dpd.DPD_plus_21_month
      ,dpd.DPD_plus_22_month
      ,dpd.DPD_plus_23_month
      ,dpd.DPD_plus_24_month
      --UPB of delinquent loans
      ,dpd.UPB_plus_1_day
      ,dpd.UPB_plus_5_day
      ,dpd.UPB_plus_10_day
      ,dpd.UPB_plus_15_day
      ,dpd.UPB_plus_1_month
      ,dpd.UPB_plus_2_month
      ,dpd.UPB_plus_3_month
      ,dpd.UPB_plus_4_month
      ,dpd.UPB_plus_5_month
      ,dpd.UPB_plus_6_month
      ,dpd.UPB_plus_7_month
      ,dpd.UPB_plus_8_month
      ,dpd.UPB_plus_9_month
      ,dpd.UPB_plus_10_month
      ,dpd.UPB_plus_11_month
      ,dpd.UPB_plus_12_month
      ,dpd.UPB_plus_13_month
      ,dpd.UPB_plus_14_month
      ,dpd.UPB_plus_15_month
      ,dpd.UPB_plus_16_month
      ,dpd.UPB_plus_17_month
      ,dpd.UPB_plus_18_month
      ,dpd.UPB_plus_19_month
      ,dpd.UPB_plus_20_month
      ,dpd.UPB_plus_21_month
      ,dpd.UPB_plus_22_month
      ,dpd.UPB_plus_23_month
      ,dpd.UPB_plus_24_month
      --Condonation by maturity    
      ,cbm.condoned_amount_plus_1_day
      ,cbm.condoned_amount_plus_5_day
      ,cbm.condoned_amount_plus_10_day
      ,cbm.condoned_amount_plus_15_day
      ,cbm.condoned_amount_1_month
      ,cbm.condoned_amount_2_month
      ,cbm.condoned_amount_3_month
      ,cbm.condoned_amount_4_month
      ,cbm.condoned_amount_5_month
      ,cbm.condoned_amount_6_month
      ,cbm.condoned_amount_7_month
      ,cbm.condoned_amount_8_month
      ,cbm.condoned_amount_9_month
      ,cbm.condoned_amount_10_month
      ,cbm.condoned_amount_11_month
      ,cbm.condoned_amount_12_month
      ,cbm.condoned_amount_13_month
      ,cbm.condoned_amount_14_month
      ,cbm.condoned_amount_15_month
      ,cbm.condoned_amount_16_month
      ,cbm.condoned_amount_17_month
      ,cbm.condoned_amount_18_month
      ,cbm.condoned_amount_19_month
      ,cbm.condoned_amount_20_month
      ,cbm.condoned_amount_21_month
      ,cbm.condoned_amount_22_month
      ,cbm.condoned_amount_23_month
      ,cbm.condoned_amount_24_month
from payment_dates pd
left join dpd_w_dates dpd
on pd.loan_id = dpd.loan_id
left join condonation_by_mob cbm
on pd.loan_id = cbm.loan_id
