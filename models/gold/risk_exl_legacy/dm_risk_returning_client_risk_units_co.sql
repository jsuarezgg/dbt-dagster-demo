{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--- QUERY CREATED BY:  RISK TEAM - MANOEL BOMFIM (BR) + EXTERNAL (PROCLINK) - Recommendations and overview by Carlos D.A. Puerto N. (AE - Data team)
--- Description: This datamart is part of the process of deploying Looker across the entire company (2023Q1)
--- LAST UPDATE: @2023-03-24

with risk_lvl as (
  select
    client_id,
    first_value(risk_level, TRUE) as first_risk
  from {{ source('risk','jfr_pre_ECM_table') }}
  group by 1
),

addicupo_final as (
  SELECT *
  FROM
    (
    select *,
           row_number() over (partition by client_id, addicupo_last_update order by addicupo_last_update desc) as rn
    from {{ref('f_syc_clients_addicupo_history_co')}}
    )
  WHERE rn = 1
)
,
addicupo_history as(
  select
    case when addicupo_last_update_v1 is null then current_date() - 1
         else addicupo_last_update_v1
    end as addicupo_last_update_v2,
    *
  from
    (
      select
        lead(addicupo_last_update) over (partition by client_id order by addicupo_last_update) as addicupo_last_update_v1,
        id,
        client_id,
        total_addicupo,
        update_addicupo_reason,
        addicupo_state,
        addicupo_last_update,
        update_addicupo_source,
        static_addicupo,
        remaining_addicupo,
        addicupo_state_v2,
        addicupo_state_v2_reason
      from
        addicupo_final
    )
),

--select * from addicupo_history
--where client_id = '00001732-862a-44ac-a916-59efb5452a26';

score_data as(
  select  *  from
    (
      (
      select
        current_execution_date,
        client_id,
        proba_pd_rc
      from hive_metastore.ds.back_test_prediction_202202_202211_rc_co_v2
      )
      union all
      (
      select
        current_execution_date,
        client_id,
        proba_pd_rc
      from {{ source('risk','predictions_returning_client_model_cl_level') }}
      where current_execution_date > '2022-11-09'
       )
    )
)
,
pre_rc_model as (
  select
    client_id,
    current_execution_date,
    proba_pd_rc as score,
    ntile(5) over( partition by current_execution_date order by proba_pd_rc) as quantile,
    ntile(10) over( partition by current_execution_date order by proba_pd_rc) as decile,
    ntile(20) over( partition by current_execution_date order by proba_pd_rc) as twentile
  from  score_data
  where current_execution_date = '2022-02-01'
)
,
quantile_cutoff AS
  ( SELECT quantile,
           round( min( CASE WHEN quantile = 1 THEN 0 ELSE score END ), 3 ) AS min_score,
           round( CASE WHEN quantile = 5 THEN 1 ELSE lead( min( CASE WHEN quantile = 1 THEN 0 ELSE score END ) ) over (ORDER BY quantile ) END, 3 ) AS max_score
   FROM pre_rc_model
   GROUP BY 1),
decile_cutoff AS
  ( SELECT decile,
       round( min( CASE WHEN decile = 1 THEN 0 ELSE score END ), 3 ) AS min_score,
       round( CASE WHEN decile = 10 THEN 1 ELSE lead( min( CASE WHEN decile = 1 THEN 0 ELSE score END ) ) over (ORDER BY decile ) END, 3 ) AS max_score
   FROM pre_rc_model
   GROUP BY 1),
twentile_cutoff AS (
SELECT twentile,
    round( min( CASE WHEN twentile = 1 THEN 0 ELSE score END ), 3 ) AS min_score,
    round( CASE WHEN twentile = 20 THEN 1 ELSE lead( min( CASE WHEN twentile = 1 THEN 0 ELSE score END ) ) over (ORDER BY twentile ) END, 3 ) AS max_score
  FROM pre_rc_model
  GROUP BY 1
),


risk_master_table_latest_available_rc_model_score AS (
SELECT
    a.prospect_id,
    a.d_vintage as application_date,
    MAX(b.current_execution_date) AS date_most_recent_score_available,
    element_at(array_sort(array_agg(CASE WHEN b.proba_pd_rc is not null then struct(b.current_execution_date, b.proba_pd_rc) else NULL end), (left, right) -> case when left.current_execution_date < right.current_execution_date then 1 when left.current_execution_date > right.current_execution_date then -1 when left.current_execution_date == right.current_execution_date then 0 end), 1).proba_pd_rc as most_recen_proba_pd_rc
  from {{ source('gold','risk_master_table_co')}} AS a
  INNER JOIN score_data b on a.prospect_id = b.client_id and a.d_vintage >= b.current_execution_date and datediff(a.d_vintage, b.current_execution_date) < 40 AND a.client_type = 'CLIENT'
  GROUP BY 1,2
)
--select * from risk_master_table_latest_available_rc_model_score where prospect_id = '00001732-862a-44ac-a916-59efb5452a26';

select
  'co' as country_code,
  a.application_id,
  a.loan_id,
  a.loan_originated,
  a.prospect_id,
  a.store_user_id,
  a.application_date_time,
  a.d_vintage,
  a.w_vintage,
  a.m_vintage,
  a.q_vintage,
  a.product,
  a.amount,
  a.approved_amount,
  a.remaining_addicupo,
  a.requested_amount,
  a.amount_before_discount,
  a.preapproval_amount,
  a.term,
  b.date_most_recent_score_available as latest_date_before_transaction,
  b.most_recen_proba_pd_rc as RC_PD_score_before_transaction,
  c.total_addicupo as current_total_addicupo,
  e.total_addicupo as total_addicupo_during_loan,
  c.remaining_addicupo as current_remaining_addicupo,
  e.remaining_addicupo as remaining_addicupo_during_loan,
  c.addicupo_last_update as current_addicupo_last_update,
  e.addicupo_last_update as addicupo_last_update_during_loan,
  c.addicupo_state_v2 as current_addicupo_state_v2,
  e.addicupo_state_v2 as addicupo_state_v2_during_loan,
  c.addicupo_state_v2_reason as current_addicupo_state_v2_reason,
  e.addicupo_state_v2_reason as addicupo_state_v2_reason_during_loan,
  c.is_transactional_based as is_transactional_based,
  d.first_risk,
  a.interest_rate,
  a.mdf,
  a.fga,
  a.fga_effective,
  a.ally_slug,
  a.ally_name,
  a.store_name,
  a.ally_brand,
  a.ally_vertical,
  a.ally_is_terminated,
  a.ally_terminated_date,
  a.ally_region,
  a.id_exp_city,
  a.id_exp_region,
  a.addi_pd_multiplied,
  a.addi_pd,
  a.pd_multiplier,
  a.bureau_pd,
  a.credit_score,
  a.credit_score_name,
  a.journey_name,
  a.journey_stage_name,
  a.stage,
  a.journey_stages,
  a.stages,
  a.credit_status_reason,
  a.credit_policy_name,
  a.campaign_id,
  a.marketing_channel,
  a.ia_loan,
  a.client_type,
  a.imply_agg,
  a.preapproval_client,
  a.preapproval_application,
  a.application_channel,
  a.application_number,
  a.loan_number,
  a.classification_at_origination,
  a.current_status_calc_date,
  a.mob,
  a.is_fully_paid,
  a.fully_paid_date,
  a.n_previous_fully_paid_loans,
  a.is_cancelled,
  a.cancellation_date,
  a.cancellation_reason,
  a.fraud_write_off,
  a.paid_installments,
  a.total_principal_paid,
  a.days_past_due,
  a.unpaid_principal,
  a.unpaid_interest,
  a.unpaid_guarantee,
  a.principal_condoned,
  a.interest_condoned,
  a.guarantee_condoned,
  a.max_days_past_due,
  a.dq_buckets,
  a.first_payment_date,
  a.FP_date_plus_1_day,
  a.FP_date_plus_5_day,
  a.FP_date_plus_10_day,
  a.FP_date_plus_15_day,
  a.FP_date_plus_1_month,
  a.FP_date_plus_2_month,
  a.FP_date_plus_3_month,
  a.FP_date_plus_4_month,
  a.FP_date_plus_5_month,
  a.FP_date_plus_6_month,
  a.DPD_plus_1_day,
  a.DPD_plus_5_day,
  a.DPD_plus_10_day,
  a.DPD_plus_15_day,
  a.DPD_plus_1_month,
  a.DPD_plus_2_month,
  a.DPD_plus_3_month,
  a.DPD_plus_4_month,
  a.DPD_plus_5_month,
  a.DPD_plus_6_month,
  a.UPB_plus_1_day,
  a.UPB_plus_5_day,
  a.UPB_plus_10_day,
  a.UPB_plus_15_day,
  a.UPB_plus_1_month,
  a.UPB_plus_2_month,
  a.UPB_plus_3_month,
  a.UPB_plus_4_month,
  a.UPB_plus_5_month,
  a.UPB_plus_6_month,
  f.quantile,
  g.decile,
  h.twentile,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at

from      {{ source('gold','risk_master_table_co')}}         AS a
left join risk_master_table_latest_available_rc_model_score AS b on a.prospect_id = b.prospect_id and a.d_vintage = b.application_date
left join {{ ref('d_syc_clients_co') }}                      AS c on a.prospect_id = c.client_id
left join risk_lvl                                           AS d on a.prospect_id = d.client_id
left join addicupo_history                                   AS e on a.prospect_id = e.client_id and (a.d_vintage >= e.addicupo_last_update and a.d_vintage < e.addicupo_last_update_v2)
left join quantile_cutoff                                    AS f on b.most_recen_proba_pd_rc < f.max_score and b.most_recen_proba_pd_rc >= f.min_score
left join decile_cutoff                                      AS g on b.most_recen_proba_pd_rc < g.max_score and b.most_recen_proba_pd_rc >= g.min_score
left join twentile_cutoff                                    AS h on b.most_recen_proba_pd_rc < h.max_score and b.most_recen_proba_pd_rc >= h.min_score
