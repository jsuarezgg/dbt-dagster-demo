{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH rc_pd_scores AS (
  SELECT 
    client_id
    , current_execution_date
    , proba_pd_rc
  FROM {{ source('risk', 'predictions_returning_client_model_cl_level') }}
  WHERE current_execution_date > '2022-11-09'

  UNION ALL

  SELECT 
    client_id
    , current_execution_date
    , proba_pd_rc
  FROM hive_metastore.ds.back_test_prediction_202202_202211_rc_co_v2
  )

, score_data AS (
  SELECT 
    a.loan_id
    , max(a.prospect_id) AS prospect_id
    , max(a.d_vintage) AS d_vintage
    , max(a.w_vintage) AS w_vintage
    , max(a.loan_number) AS loan_number
    , max(a.product) AS product
    , max(a.addi_pd) AS addi_pd
    , max(CASE WHEN a.d_vintage > b.current_execution_date THEN b.current_execution_date END) AS new_rc_pd_bef_orig
    , min(CASE WHEN a.d_vintage < b.current_execution_date THEN b.current_execution_date END) AS new_rc_pd_aft_orig
  FROM {{ ref('risk_master_table_co') }} a
  LEFT JOIN rc_pd_scores b ON a.prospect_id = b.client_id AND months_between(a.d_vintage,b.current_execution_date) BETWEEN -1 AND 1
  WHERE true
  AND loan_number > 0
  AND d_vintage > '2022-01-20'
  GROUP BY 1
  )

SELECT 
    a.loan_id
    , a.prospect_id AS client_id
    , a.d_vintage
    , a.loan_number
    , a.product
    , a.addi_pd
    , coalesce(d1.proba_pd_rc) AS new_rc_pd_bf
    , coalesce(d2.proba_pd_rc) AS new_rc_pd_aft
FROM score_data a 
LEFT JOIN rc_pd_scores d1 ON a.prospect_id = d1.client_id AND d1.current_execution_date = a.new_rc_pd_bef_orig
LEFT JOIN rc_pd_scores d2 ON a.prospect_id = d2.client_id AND d2.current_execution_date = a.new_rc_pd_aft_orig
