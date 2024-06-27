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
--- LAST UPDATE: @2023-02-23

with score_data as(
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
data_score_addicupo as (
  select
    a.calculation_date,
    a.client_id,
    a.cohort_month,
    a.cohort_day,
    a.first_product,
    a.mob,
    a.last_mob_day,
    a.upb,
    a.upb_adj,
    a.dpd,
    a.max_prev_dpd,
    a.loans_originated,
    a.cumulative_loans_mob,
    a.cumulative_loans,
    a.gmv_originated,
    a.cumulative_gmv_mob,
    a.cumulative_gmv,
    a.mdf_originated,
    a.cumulative_mdf_mob,
    a.cumulative_mdf,
    a.last_orig_date,
    a.current_active_loans,
    a.unpaid_loans,
    a.loans_paid_full,
    a.gmv_paid_full,
    a.cancelled_loans_flag,
    a.addicupo,
    a.addicupo_adj,
    a.available_addicupo,
    a.available_addicupo_adj,
    a.cupo_state,
    a.first_dq30_date,
    a.prev_dq30,
    a.first_dq60_date,
    a.prev_dq60,
    a.first_dq90_date,
    a.prev_dq90,
    a.first_dq120_date,
    a.prev_dq120,
    a.first_dq30_mob,
    a.first_dq60_mob,
    a.first_dq90_mob,
    a.first_dq120_mob,
    a.prev_dq30_mob,
    a.prev_dq60_mob,
    a.prev_dq90_mob,
    a.prev_dq120_mob,
    a.dq30_upb,
    a.dq30_upb_mob,
    a.dq60_upb,
    a.dq60_upb_mob,
    a.dq90_upb,
    a.dq90_upb_mob,
    a.dq120_upb,
    a.dq120_upb_mob,
    a.dq30_loans,
    a.dq30_loans_mob,
    a.dq60_loans,
    a.dq60_loans_mob,
    a.dq90_loans,
    a.dq90_loans_mob,
    a.dq120_loans,
    a.dq120_loans_mob,
    lag(a.addicupo_adj) over ( partition by a.client_id order by a.calculation_date) as previous_addicupo_adj,
    lag(a.cupo_state) over ( partition by a.client_id order by a.calculation_date) as previous_cupo_state,
    b.date_most_recent_score_available as latest_date_before_transaction,
    b.most_recen_proba_pd_rc as prob_score_before_transaction
  from      {{ source('risk','daily_client_data_co') }} AS a
  --left join sandbox.risk_daily_client_data_lastest_available_rc_model_score b on a.client_id = b.client_id and a.calculation_date = b.daily_client_data_calculation_date
  left join {{ ref('dm_risk_daily_client_data_lastest_available_rc_model_score_co') }} AS b on a.client_id = b.client_id and a.calculation_date = b.daily_client_data_calculation_date
),
data_score_addicupo_2 as (
  select
    a.*,
    case
      when a.addicupo_adj = a.previous_addicupo_adj and a.cupo_state = 'AVAILABLE'             THEN 'no_change'
      when a.cupo_state = 'AVAILABLE'               and a.previous_cupo_state = 'FROZEN'       THEN 'unfreeze'
      when a.addicupo_adj > a.previous_addicupo_adj and a.cupo_state = 'AVAILABLE'             THEN 'increase'
      when a.addicupo_adj < a.previous_addicupo_adj and a.cupo_state = 'AVAILABLE'             THEN 'decrease'
      when a.cupo_state = 'FROZEN'                  and a.previous_cupo_state <> 'FROZEN'      THEN 'freeze'
      when a.cupo_state = 'FROZEN'                  and a.previous_cupo_state = 'FROZEN'       THEN 'no_change'
      when a.cupo_state is NULL                     and a.previous_cupo_state is not null      THEN NULL
      when a.cupo_state is NULL                     and a.previous_cupo_state is null          THEN 'no_change'
      when a.cupo_state = 'CANCELED'                and a.previous_cupo_state <> 'CANCELED'    THEN 'canceled'
      when a.cupo_state = 'CANCELED'                and a.previous_cupo_state = 'CANCELED'     THEN 'no_change'
      when a.cupo_state = 'NOT_ASSIGNED'            and a.previous_cupo_state <> 'NOT_ASSIGNED'THEN 'not_assigned'
      when a.cupo_state = 'NOT_ASSIGNED'            and a.previous_cupo_state is null          THEN 'not_assigned'
      when a.cupo_state = 'FROZEN'                  and a.previous_cupo_state is null          THEN 'freeze'
      when a.cupo_state = 'CANCELED'                and a.previous_cupo_state is null          THEN 'canceled'
      when a.cupo_state = 'NOT_ASSIGNED'            and a.previous_cupo_state = 'NOT_ASSIGNED' THEN 'no_change'
      else a.cupo_state
    end as exposure_status,
    c.quantile,
    d.decile,
    e.twentile
  from data_score_addicupo a
  left join quantile_cutoff c on a.prob_score_before_transaction < c.max_score  and a.prob_score_before_transaction >= c.min_score
  left join decile_cutoff d   on a.prob_score_before_transaction < d.max_score  and a.prob_score_before_transaction >= d.min_score
  left join twentile_cutoff e on a.prob_score_before_transaction < e.max_score  and a.prob_score_before_transaction >= e.min_score
)

select
    'co' as country_code,
    *,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
from data_score_addicupo_2
