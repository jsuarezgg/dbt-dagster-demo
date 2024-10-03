{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--        materialized='incremental',
--        unique_key='application_id',
--        incremental_strategy='merge',
-- AE - Carlos D. Puerto: Incremental approach as the one we use with the silver builder
WITH
{%- if is_incremental() %}
target_applications_co AS (
    SELECT DISTINCT application_id
    FROM {{ source('silver_live', 'f_applications_co') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
{%- endif %}
f_refinance_loans_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_refinance_loans_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
bl_application_channel_co AS (
    SELECT *
    FROM {{ ref('bl_application_channel') }}
    WHERE country_code = 'CO'
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_approval_loans_to_refinance_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_approval_loans_to_refinance_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_approval_loans_to_refinance_co_logs AS (
    SELECT *
    FROM {{ source('silver_live', 'f_approval_loans_to_refinance_co_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_loans_to_refinance_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_applications_loans_to_refinance_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,

--risk_master_table_co AS (
--    SELECT DISTINCT
--           loan_id,
--           dpd_plus_1_month,
--           UPB_plus_1_month,
--           approved_amount,
--           FP_date_plus_1_month
--    FROM {{ ref('risk_master_table_co') }}
--    {%- if is_incremental() %}
--    WHERE application_id IN (SELECT application_id FROM target_applications_co)
--    {%- endif -%}
--)
--,
f_applications_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_applications_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
--,
--f_loan_proposals_co AS (
--    SELECT *
--    FROM {{ source('silver_live', 'f_loan_proposals_co') }}
--    {%- if is_incremental() %}
--    WHERE application_id IN (SELECT application_id FROM target_applications_co)
--    {%- endif -%}
--)
,
f_underwriting_fraud_stage_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_underwriting_fraud_stage_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
bl_application_product_co AS (
    SELECT *
    FROM {{ ref('bl_application_product_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
bl_ally_brand_ally_slug_status_co AS (
    SELECT *
    FROM {{ ref('bl_ally_brand_ally_slug_status') }}
    WHERE country_code = 'CO'
)
,
f_approval_loans_to_refinance_co_lst AS (
    SELECT
        altr.refinanced_by_origination_of_loan_id,
        COLLECT_LIST(
            NAMED_STRUCT(
            'loan_to_refinance_id', altr.loan_id, -- = loan_to_refinance_id
            'custom_loan_refinance_id', altr.custom_loan_refinance_id, -- Hash on application_id+loan_to_refinance_id
            'outstanding_balance_total', app_ltr.outstanding_balance_total
            )
        ) AS loans_to_refinance_lst_debug
    -- To handle this particular issue: https://addico.slack.com/archives/C05A3B0KPEZ/p1700772846156439?thread_ts=1700772677.305429&cid=C05A3B0KPEZ
    FROM (SELECT refinanced_by_origination_of_loan_id,
                 loan_id,
                 custom_loan_refinance_id 
          FROM f_approval_loans_to_refinance_co
          UNION ALL
          SELECT refinanced_by_origination_of_loan_id,
                 loan_id,
                 custom_loan_refinance_id
          FROM f_approval_loans_to_refinance_co_logs
          WHERE custom_loan_refinance_id = '5f05cd7d6d94c0518d49d95a8eebf6c2'
          ) AS altr
    LEFT JOIN f_applications_loans_to_refinance_co AS app_ltr ON altr.custom_loan_refinance_id = app_ltr.custom_loan_refinance_id
    GROUP BY 1
)
SELECT
    'CO' AS country_code,
    rl.loan_id,
    rl.client_id,
    rl.application_id,
    rl.ally_slug,
    rl.store_slug,
    rl.approved_amount,
    CASE WHEN app.requested_amount_without_discount >0 THEN app.requested_amount_without_discount
         ELSE app.requested_amount
    END AS requested_amount,
    rl.term,
    rl.effective_annual_rate AS interest_rate,
    rl.total_interest,
    rl.guarantee_rate,
    rl.guarantee_total AS guarantee_amount,
    --rl.loan_type,
    from_utc_timestamp(rl.origination_date,'America/Bogota') AS origination_date_local,
    from_utc_timestamp(rl.first_payment_date,'America/Bogota') AS first_payment_date_local,
    app.client_type,
    app.journey_name,
    ac.application_channel,
    ac.synthetic_channel,
    ltr.loans_to_refinance_lst_debug,
    uw.credit_policy_name,
    uw.credit_status,
    uw.credit_status_reason,
    ap.original_product,
    ap.processed_product,
    ap.synthetic_product_category,
    ap.synthetic_product_subcategory,
    asl.ally_vertical,
    asl.ally_brand,
    COALESCE(asl.ally_cluster,'KA') AS ally_cluster,
    NOW() AS ingested_at,
    TO_TIMESTAMP('{{ var("execution_date") }}') AS updated_at

FROM      f_refinance_loans_co                  AS rl
LEFT JOIN f_applications_co                     AS app ON rl.application_id = app.application_id
LEFT JOIN f_underwriting_fraud_stage_co         AS uw  ON rl.application_id = uw.application_id
LEFT JOIN f_approval_loans_to_refinance_co_lst  AS ltr ON rl.loan_id = ltr.refinanced_by_origination_of_loan_id
LEFT JOIN bl_application_product_co             AS ap  ON rl.application_id = ap.application_id
LEFT JOIN bl_ally_brand_ally_slug_status_co     AS asl ON rl.ally_slug = asl.ally_slug
LEFT JOIN bl_application_channel_co             AS ac  ON rl.application_id = ac.application_id