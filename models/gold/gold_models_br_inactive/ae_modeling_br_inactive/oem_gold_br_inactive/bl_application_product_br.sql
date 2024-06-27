{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- AE - Carlos D. Puerto: Incremental approach as the one we use with the silver builder
WITH
{%- if is_incremental() %}
target_applications_br AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_br') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
{%- endif %}
f_originations_bnpl_br AS (
    SELECT *
    FROM {{ ref('f_originations_bnpl_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_originations_bnpn_br AS (
    SELECT *
    FROM {{ ref('f_originations_bnpn_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_applications_br AS (
    SELECT *
    FROM {{ ref('f_applications_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_underwriting_fraud_stage_br AS (
    SELECT *
    FROM {{ ref('f_underwriting_fraud_stage_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_loan_proposals_br AS (
    SELECT *
    FROM {{ ref('f_loan_proposals_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_origination_events_br_logs AS (
    SELECT *
    FROM {{ ref('f_origination_events_br_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
originations_br AS (
  SELECT
    application_id,
    loan_id
  FROM f_originations_bnpl_br
  UNION ALL
  SELECT
    application_id,
    NULL AS loan_id
  FROM f_originations_bnpn_br
)
,
criteria AS (
  SELECT
    a.application_id,
    COALESCE(a.application_date,a.last_event_ocurred_on_processed) AS application_date,
    COALESCE(a.journey_name,oe.journey_name) AS orig_v2_journey_name,
    COALESCE(a.product,oe.product) AS orig_v2_application_product,
    (COALESCE(a.product,oe.product) IS NULL) AS orig_v2_product_is_null,
    COALESCE(a.custom_platform_version, CASE WHEN a.last_event_ocurred_on_processed < '2021-05_31'::DATE THEN 'LEGACY' ELSE 'ORIGINATIONS_V2' END) AS custom_platform_version,
    COALESCE(a.application_date,a.last_event_ocurred_on_processed) < ('2022-11-08 14:05')::timestamp AS is_cupo_window_preaddi2_0,
    COALESCE(lp.num_loan_proposals,0)>0 AS had_loan_proposals,
    COALESCE(a.custom_is_bnpn_branched, FALSE) AS custom_is_bnpn_branched,
    (o.application_id IS NOT NULL) AS has_originated,
    COALESCE(orig_lp.origination_has_total_interest,FALSE) AS origination_has_total_interest
  FROM      f_applications_br AS a
  LEFT JOIN originations_br AS o ON a.application_id = o.application_id
  LEFT JOIN f_underwriting_fraud_stage_br AS uwf ON a.application_id = uwf.application_id
  LEFT JOIN (
            SELECT application_id, num_loan_proposals, num_loan_proposals_w_interest, (num_loan_proposals-num_loan_proposals_w_interest) AS num_loan_proposals_wo_interest
            FROM (SELECT application_id, count(1) AS num_loan_proposals, COALESCE(count(1) FILTER (WHERE COALESCE(total_interest,0)>0),0) AS num_loan_proposals_w_interest FROM f_loan_proposals_br GROUP BY 1)
  ) AS lp ON lp.application_id = a.application_id
  LEFT JOIN (SELECT application_id, FIRST_VALUE(journey_name,TRUE) AS journey_name, FIRST_VALUE(product,TRUE) AS product FROM f_origination_events_br_logs GROUP BY 1
  ) AS oe ON oe.application_id = a.application_id
  LEFT JOIN (
    SELECT o.application_id, o.loan_id, COALESCE(lp.total_interest,0)>0 origination_has_total_interest
    FROM       originations_br AS o
    INNER JOIN f_loan_proposals_br AS lp ON o.loan_id = lp.loan_proposal_id
    ) AS orig_lp ON a.application_id = orig_lp.application_id
  WHERE a.application_id IS NOT NULL
)
,
step_1_application_processed AS (
  SELECT
    *,
    CASE
      WHEN custom_platform_version = 'LEGACY' THEN 'PAGO_BR'
      WHEN custom_platform_version = 'ORIGINATIONS_V2' THEN
        (CASE
        WHEN orig_v2_product_is_null THEN 'JOURNEY_NOT_PROCESSED'
        WHEN orig_v2_application_product = 'PAGO_BR' THEN
          (CASE
          WHEN custom_is_bnpn_branched IS TRUE THEN 'BNPN_BR'
          ELSE 'PAGO_BR'
          END)
        WHEN orig_v2_application_product = 'BNPN_BR' THEN 'BNPN_BR'
        ELSE 'ERROR_ORIGINATIONS_V2'
        END)
      ELSE 'ERROR_CUSTOM_PLATFORM_VERSION'
     END AS processed_product
  FROM criteria
)
,
step_2_product_category AS (
  SELECT
    * ,
    CASE
      WHEN processed_product = 'PAGO_BR' THEN (
         CASE
          WHEN is_cupo_window_preaddi2_0 THEN 'BNPL_BR'
          WHEN had_loan_proposals THEN 'BNPL_BR'
          WHEN had_loan_proposals IS NOT TRUE THEN 'GENERIC'
          ELSE 'ERROR_PAGO' END
        )
      WHEN processed_product = 'BNPN_BR' THEN 'BNPN_BR'
      WHEN processed_product = 'JOURNEY_NOT_PROCESSED' THEN 'JOURNEY_NOT_PROCESSED'
      ELSE 'ERROR'
    END AS synthetic_product_category
  FROM step_1_application_processed
)
,
step_3_product_sub_category AS (
  SELECT
    *,
    CASE
      WHEN synthetic_product_category = 'BNPL_BR' THEN (
        CASE
          WHEN (has_originated AND origination_has_total_interest) THEN 'BNPL_BR Flex'
          WHEN (has_originated AND origination_has_total_interest IS NOT TRUE) THEN 'BNPL_BR 0%'
          WHEN has_originated IS NOT TRUE THEN 'BNPL_BR Generic'
          ELSE 'ERROR_BNPL_BR'
        END
      )
      WHEN synthetic_product_category = 'BNPN_BR' THEN 'BNPN_BR'
      WHEN synthetic_product_category = 'JOURNEY_NOT_PROCESSED' THEN 'JOURNEY_NOT_PROCESSED'
      WHEN synthetic_product_category = 'GENERIC' THEN 'GENERIC'
      ELSE 'ERROR'
    END AS synthetic_product_subcategory
  FROM step_2_product_category
)

SELECT
  application_id,
  orig_v2_application_product AS original_product,
  processed_product,
  synthetic_product_category,
  synthetic_product_subcategory,
  NOT (processed_product ILIKE '%error%' OR synthetic_product_category ILIKE '%error%' OR synthetic_product_subcategory ILIKE '%error%') AS debug_is_ok,
  NAMED_STRUCT('custom_platform_version',custom_platform_version,
              'synthetic_application_date',application_date,
              'journey_name',orig_v2_journey_name,
              'is_cupo_window_preaddi2_0',is_cupo_window_preaddi2_0,
              'custom_is_bnpn_branched',custom_is_bnpn_branched,
              'had_loan_proposals',had_loan_proposals,
              'has_originated',has_originated,
              'origination_has_total_interest',origination_has_total_interest) AS debug_data,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM step_3_product_sub_category
