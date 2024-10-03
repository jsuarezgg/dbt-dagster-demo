{{
    config(
        materialized='incremental',
        unique_key='application_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
-- AE - Carlos D. Puerto: Incremental approach as the one we use with the silver builder
WITH
{%- if is_incremental() %}
target_applications_co AS (
    SELECT DISTINCT application_id
    FROM {{ source('silver_live', 'f_applications_co') }}
    WHERE CAST(last_event_ocurred_on_processed AS DATE) BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
{%- endif %}
f_applications_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_applications_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_originations_bnpl_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_originations_bnpl_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
),
f_originations_bnpn_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_originations_bnpn_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_underwriting_fraud_stage_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_underwriting_fraud_stage_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_loan_proposals_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_loan_proposals_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_origination_events_co_logs AS (
    SELECT *
    FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_addi_v2_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_applications_addi_v2_co') }}
    WHERE (product_v2 IS NOT NULL OR evaluation_type IS NOT NULL)
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_refinance_loans_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_refinance_loans_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
criteria AS (
  SELECT
    a.application_id,
    COALESCE(a.application_date,a.last_event_ocurred_on_processed) AS application_date,
    COALESCE(a.journey_name,oe.journey_name) AS orig_v2_journey_name,
    COALESCE(a.product,oe.product) AS orig_v2_application_product,
    (COALESCE(a.product,oe.product) IS NULL) AS orig_v2_product_is_null,
    COALESCE(a.custom_platform_version, CASE WHEN a.last_event_ocurred_on_processed < '2021-06_08'::DATE THEN 'LEGACY' ELSE 'ORIGINATIONS_V2' END) AS custom_platform_version,
    COALESCE(a.application_date,a.last_event_ocurred_on_processed) < ('2023-04-03 15:45')::timestamp AS is_cupo_window_preaddi2_0,
    v_two.evaluation_type AS evaluation_type,
    CASE WHEN v_two.product_v2 = 'FLEX' THEN NULL
         WHEN v_two.product_v2 = 'EXTRA' THEN 'GRANDE'
         ELSE v_two.product_v2
    END AS product_v2_from_events, -- After inspecting the ones marked as FLEX, some of them look to be CUPO, other as INTRO (according to their loan proposals). For this reason  we let the business logic assign it; 54K CUPO, 1K INTRO
    COALESCE(lp.num_loan_proposals,0)>0 AS had_loan_proposals,
    lp.num_loan_proposals,
    lp.num_loan_proposals_w_interest,
    lp.num_loan_proposals_wo_interest,
    COALESCE(oe.journey_name ilike '%pre%appr%',FALSE) AS is_preapproval_journey,
    COALESCE(uwf.credit_policy_name in ('addipago_0aprfga_policy','addipago_0fga_policy','addipago_claro_policy','addipago_mario_h_policy','addipago_no_history_policy','addipago_policy','addipago_policy_amoblando','adelante_policy_pago','closing_policy_pago','default_policy_pago','finalization_policy_pago','rc_0aprf     ga','rc_0fga','rc_addipago_policy_amoblando','rc_adelante_policy','rc_closing_policy','rc_finalization_policy','rc_pago_0aprfga','rc_pago_0fga','rc_pago_claro','rc_pago_mario_h','rc_pago_standard','rc_rejection_policy','rc_standard','rejection_policy_pago'),FALSE) AS is_legacy_pago_credit_policy,
    COALESCE(uwf.credit_policy_name in ('closing_policy','finalization_policy', 'rejection_policy'),FALSE) AS is_legacy_pago_subset_credit_policy,
    (COALESCE(o.term,rl.term)=3) AS is_legacy_origination_term,
    COALESCE(ARRAYS_OVERLAP(ARRAY('underwriting-co','underwriting-psychometric-co'), oe.stage_names) AND oe.journey_name ILIKE '%santander%',FALSE) AS is_actually_addi_from_santander,
    (COALESCE(o.application_id, rl.application_id, pn.application_id) IS NOT NULL) AS has_originated,
    COALESCE(orig_lp.origination_has_total_interest,FALSE) AS origination_has_total_interest,
    (rl.application_id IS NOT NULL OR a.channel = 'REFINANCE') AS is_refinance_application

    FROM f_applications_co AS a
    LEFT JOIN f_originations_bnpl_co AS o ON a.application_id = o.application_id
    LEFT JOIN f_originations_bnpn_co AS pn ON a.application_id = pn.application_id
    LEFT JOIN f_refinance_loans_co   AS rl ON a.application_id = rl.application_id
    LEFT JOIN f_underwriting_fraud_stage_co AS uwf ON a.application_id = uwf.application_id
    LEFT JOIN f_applications_addi_v2_co AS v_two ON a.application_id = v_two.application_id
    -- Loan proposals flags
    LEFT JOIN (
              SELECT application_id, num_loan_proposals, num_loan_proposals_w_interest, (num_loan_proposals-num_loan_proposals_w_interest) AS num_loan_proposals_wo_interest
              FROM (SELECT application_id, count(1) AS num_loan_proposals, COALESCE(count(1) FILTER (WHERE COALESCE(total_interest,0)>0),0) AS num_loan_proposals_w_interest FROM f_loan_proposals_co GROUP BY 1)
    ) AS lp ON lp.application_id = a.application_id
    -- Getting journey name, product and stages in each application
    LEFT JOIN (SELECT application_id, FIRST_VALUE(journey_name,TRUE) AS journey_name, FIRST_VALUE(product,TRUE) AS product, collect_set(journey_stage_name) as stage_names
               FROM f_origination_events_co_logs GROUP BY 1
    ) AS oe ON oe.application_id = a.application_id
    -- Originations total interest flag
    LEFT JOIN (
      SELECT o_lr.application_id, o_lr.loan_id, COALESCE((lp.total_interest)::double,o_lr.total_interest,0)>0 origination_has_total_interest
      FROM -- BNPL Loans and Loan Refinance loans
        (   (SELECT application_id, loan_id, NULL AS total_interest FROM f_originations_bnpl_co) -- total interest not available in this table
         UNION ALL
            (SELECT application_id, loan_id, total_interest  FROM f_refinance_loans_co)
         ) AS o_lr
      INNER JOIN f_loan_proposals_co AS lp ON o_lr.loan_id = lp.loan_proposal_id
      ) AS orig_lp ON a.application_id = orig_lp.application_id
    -- Ignore 1 case with a null application_id (sometimes represented in several rows in the source)
    WHERE a.application_id IS NOT NULL
)
,
step_1_application_processed AS (
  SELECT
    *,
    CASE
    WHEN custom_platform_version = 'LEGACY' THEN
      (CASE
      WHEN is_legacy_pago_credit_policy OR (is_legacy_pago_subset_credit_policy AND is_legacy_origination_term) THEN 'PAGO_CO'
      ELSE 'FINANCIA_CO'
      END)
    WHEN custom_platform_version = 'ORIGINATIONS_V2' THEN (
      CASE
      WHEN orig_v2_product_is_null THEN 'JOURNEY_NOT_PROCESSED'
      WHEN orig_v2_application_product = 'FINANCIA_CO' THEN
        (CASE
        WHEN orig_v2_journey_name ILIKE '%SANTANDER%' AND is_actually_addi_from_santander IS NOT TRUE THEN 'SANTANDER_CO'
        ELSE 'FINANCIA_CO'
        END)
      WHEN orig_v2_application_product = 'PAGO_CO' THEN
        (CASE
        WHEN evaluation_type ='FINANCIA' THEN 'FINANCIA_CO'
        ELSE 'PAGO_CO'
        END)
      WHEN orig_v2_application_product = 'BNPN_CO' THEN 'BNPN_CO'
      ELSE 'ERROR_ORIGINATIONS_V2'
      END
      )
    ELSE 'ERROR_CUSTOM_PLATFORM_VERSION'
    END AS processed_product
  FROM criteria
)
,
step_2_product_category AS (
  SELECT
    * ,
    CASE
      WHEN processed_product IN ('FINANCIA_CO','SANTANDER_CO') THEN 'GRANDE'
      WHEN processed_product = 'BNPN_CO' THEN 'BNPN_CO'
      WHEN processed_product = 'PAGO_CO' THEN (
        CASE
          WHEN (product_v2_from_events IS NOT NULL AND is_preapproval_journey = FALSE) THEN product_v2_from_events
          WHEN is_cupo_window_preaddi2_0 THEN 'CUPO'
          WHEN had_loan_proposals AND (num_loan_proposals=num_loan_proposals_w_interest) THEN 'INTRO'
          WHEN had_loan_proposals AND (num_loan_proposals_wo_interest>0) THEN 'CUPO'
          WHEN had_loan_proposals IS NOT TRUE THEN 'GENERIC'
          ELSE 'ERROR_PAGO'
        END
      )
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
      -- WHEN synthetic_product_category = 'CUPO' AND ((has_originated AND origination_has_total_interest) OR (has_originated IS NOT TRUE AND num_loan_proposals_w_interest > 0)) THEN 'CUPO Flex'
      -- WHEN synthetic_product_category = 'CUPO' AND ((has_originated AND origination_has_total_interest IS NOT TRUE) OR (num_loan_proposals=num_loan_proposals_wo_interest)) THEN 'CUPO 0%'
      WHEN synthetic_product_category = 'CUPO' THEN (
        CASE
          WHEN (has_originated AND origination_has_total_interest) THEN 'CUPO Flex'
          WHEN (has_originated AND origination_has_total_interest IS NOT TRUE) THEN 'CUPO 0%'
          WHEN has_originated IS NOT TRUE THEN 'CUPO Generic'
          ELSE 'ERROR_CUPO'
        END
      )
      WHEN synthetic_product_category = 'BNPN_CO' THEN 'BNPN_CO'
      WHEN synthetic_product_category = 'GRANDE' THEN 'GRANDE'
      WHEN synthetic_product_category = 'INTRO' THEN 'INTRO'
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
  (processed_product ILIKE '%error%' OR synthetic_product_category ILIKE '%error%' OR synthetic_product_subcategory ILIKE '%error%') AS debug_is_ok,
   NAMED_STRUCT('custom_platform_version',custom_platform_version,
               'product_v2_from_events',product_v2_from_events,
               'synthetic_application_date',application_date,
               'journey_name',orig_v2_journey_name,
               'is_cupo_window_preaddi2_0',is_cupo_window_preaddi2_0,
               'evaluation_type',evaluation_type,
               'had_loan_proposals',had_loan_proposals,
               'num_loan_proposals',num_loan_proposals,
               'num_loan_proposals_w_interest',num_loan_proposals_w_interest,
               'num_loan_proposals_wo_interest',num_loan_proposals_wo_interest,
               'is_legacy_pago_credit_policy',is_legacy_pago_credit_policy,
               'is_legacy_pago_subset_credit_policy',is_legacy_pago_subset_credit_policy,
               'is_legacy_origination_term',is_legacy_origination_term,
               'is_actually_addi_from_santander',is_actually_addi_from_santander,
               'has_originated',has_originated,
               'origination_has_total_interest',origination_has_total_interest,
               'is_refinance_application',is_refinance_application) AS debug_data,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM step_3_product_sub_category
