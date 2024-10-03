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
-- BL Documentation (Please update after relevant changes to thsi data product) - Notion page: https://www.notion.so/addico/gold-bl_originations_marketplace_suborders_to_originations_co-34a7efdf172a44e08204d305ca35b77d?pvs=4
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
f_applications_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_applications_co') }}
    {%- if is_incremental() %}
    WHERE  application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_originations_bnpl_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_originations_bnpl_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_originations_bnpn_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_originations_bnpn_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_marketplace_suborders_co AS (
    SELECT
        *,
        CASE WHEN suborder_total_amount_without_discount > 0 THEN suborder_total_amount_without_discount ELSE suborder_total_amount END AS synthetic_suborder_total_amount
    FROM {{ source('silver_live', 'f_applications_marketplace_suborders_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_marketplace_allies_product_policies_co AS (
    SELECT *
    FROM {{ source('silver_live', 'f_applications_marketplace_allies_product_policies_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
bl_product_policy_to_product_co AS (
    SELECT *
    FROM {{ ref('bl_product_policy_to_product_co') }}
)
,
applications_backfill_co AS (
    SELECT
        application_id,
        FIRST_VALUE(ally_slug,TRUE) AS ally_slug,
        FIRST_VALUE(channel,TRUE) AS channel,
        FIRST_VALUE(client_id,TRUE) AS client_id,
        FIRST_VALUE(client_type,TRUE) AS client_type,
        FIRST_VALUE(journey_name,TRUE) AS journey_name
    FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {% endif %}
    GROUP BY 1
)
,f_applications_marketplace_allies_product_policies_co_filtered_to_applicable AS (
    SELECT afmappc.*
    FROM      f_applications_marketplace_allies_product_policies_co AS afmappc
    LEFT JOIN bl_product_policy_to_product_co AS bl_pptpc ON afmappc.product_policy_type = bl_pptpc.ally_product_policy
    -- Filtering only the application product policy type translated to a product value (original product)
    WHERE bl_pptpc.original_product = afmappc.product
)
,
f_originations_co AS (
    (
    SELECT
        application_id,
        loan_id,
        client_id,
        ally_slug,
        origination_date,
        approved_amount AS origination_approved_amount,
        guarantee_rate AS origination_guarantee_rate,
        term AS origination_term
    FROM f_originations_bnpl_co
    )
    UNION ALL
    (
    SELECT
        application_id,
        NULL AS loan_id,
        client_id,
        ally_slug,
        last_event_ocurred_on_processed AS origination_date,
        payment_amount AS origination_approved_amount,
        NULL AS origination_guarantee_rate,
        NULL AS origination_term
    FROM f_originations_bnpn_co
    )
)
--- BOTH: BNPL + BNPN
,
origination_grouping AS (
	SELECT
	    mktp_so.application_id,
	   	o.loan_id,
	    COALESCE(mktp_so.client_id, o.client_id, bf.client_id) AS client_id,
		a.product AS original_product,
	    COUNT(mktp_so.custom_application_suborder_pairing_id) AS debug_num_suborders,
	   	--MAX(a.requested_amount) AS application_requested_amount,
	    --MAX(o.origination_approved_amount) AS origination_approved_amount,
	    SUM(mktp_so.synthetic_suborder_total_amount) AS synthetic_origination_requested_amount,
	    SUM(mktp_so.suborder_marketplace_purchase_fee * mktp_so.synthetic_suborder_total_amount) AS synthetic_origination_marketplace_purchase_fee_amount,
	    SUM(app_ftp.product_policy_cancellation_mdf   * mktp_so.synthetic_suborder_total_amount) AS synthetic_origination_cancellation_mdf_amount,
		SUM(app_ftp.product_policy_fraud_mdf          * mktp_so.synthetic_suborder_total_amount) AS synthetic_origination_fraud_mdf_amount,
	    SUM(app_ftp.product_policy_origination_mdf    * mktp_so.synthetic_suborder_total_amount) AS synthetic_origination_origination_mdf_amount,
	  	COLLECT_LIST(
			STRUCT(
			    mktp_so.custom_application_suborder_pairing_id,
			    mktp_so.suborder_id,
		        mktp_so.suborder_ally_slug,
		        mktp_so.synthetic_suborder_total_amount,
	            mktp_so.suborder_attribution_weight_by_total_amount,
	            mktp_so.suborder_marketplace_purchase_fee,
	            app_ftp.product_policy_cancellation_mdf,
	            app_ftp.product_policy_fraud_mdf,
	            app_ftp.product_policy_origination_mdf
			)
		) AS debug_suborders

	FROM       f_applications_marketplace_suborders_co AS mktp_so
	LEFT JOIN  f_applications_marketplace_allies_product_policies_co_filtered_to_applicable AS app_ftp ON mktp_so.custom_application_suborder_pairing_id = app_ftp.custom_application_suborder_pairing_id
	INNER JOIN f_originations_co AS o ON mktp_so.application_id = o.application_id
	LEFT JOIN  f_applications_co AS a ON mktp_so.application_id = a.application_id
	LEFT JOIN  applications_backfill_co AS bf ON mktp_so.application_id = bf.application_id
	GROUP BY 1,2,3,4
)

-- IMPORTANT NOTE: `synthetic_origination_requested_amount` (a field that should only be used within this BL) is
--                 the same as the `synthetic_requested_amount`  EXCEPT when we had inconsistencies in the total
--                 amounts and total amounts without discounts between the application and the sum of suborders. It's
--                 key to understand this as it affects the amount for all MDFs and the marketplace purchase fee.
--                 We deployed a fix on gold.dm_originations to make sure the values are consistent at both the suborder
--                 and application-origination level. Particularly for MDF, both rate and amount should be retrieved
--                 from this BL. Impact of this change is quite low (example: origination MDF: lowered total by <50 USD)
SELECT
    application_id,
    loan_id,
    client_id,
    original_product,
    synthetic_origination_requested_amount,
    (synthetic_origination_marketplace_purchase_fee_amount / synthetic_origination_requested_amount) AS synthetic_origination_marketplace_purchase_fee,
    synthetic_origination_marketplace_purchase_fee_amount,
    (synthetic_origination_cancellation_mdf_amount / synthetic_origination_requested_amount) AS synthetic_origination_cancellation_mdf,
    synthetic_origination_cancellation_mdf_amount,
    (synthetic_origination_fraud_mdf_amount / synthetic_origination_requested_amount) AS synthetic_origination_fraud_mdf,
    synthetic_origination_fraud_mdf_amount,
    (synthetic_origination_origination_mdf_amount / synthetic_origination_requested_amount) AS synthetic_origination_origination_mdf,
    synthetic_origination_origination_mdf_amount,
    debug_num_suborders,
    debug_suborders,
    -- Data platform columns
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM origination_grouping