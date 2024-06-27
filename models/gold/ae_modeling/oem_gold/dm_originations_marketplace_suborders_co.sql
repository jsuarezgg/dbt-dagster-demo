{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--        materialized='incremental',
--        unique_key='custom_application_suborder_pairing_id',
--        incremental_strategy='merge',
-- AE - Carlos D. Puerto: Incremental approach as the one we use with the silver builder
WITH
{%- if is_incremental() %}
target_applications_co AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_co') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
{%- endif %}
f_applications_co AS (
    SELECT *
    FROM {{ ref('f_applications_co') }}
    {%- if is_incremental() %}
    WHERE  application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_originations_bnpl_co AS (
    SELECT *
    FROM {{ ref('f_originations_bnpl_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_originations_bnpn_co AS (
    SELECT *
    FROM {{ ref('f_originations_bnpn_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_marketplace_suborders_co AS (
    SELECT *
    FROM {{ ref('f_applications_marketplace_suborders_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_marketplace_allies_product_policies_co AS (
    SELECT *
    FROM {{ ref('f_applications_marketplace_allies_product_policies_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
d_fx_rate AS (
    SELECT *
    FROM {{ source('silver', 'd_fx_rate') }}
)
,
bl_product_policy_to_product_co AS (
    SELECT *
    FROM {{ ref('bl_product_policy_to_product_co') }}
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
bl_application_channel_co AS (
    SELECT *
    FROM {{ ref('bl_application_channel') }}
    WHERE country_code = 'CO'
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
bl_ally_brand_ally_slug_status_co AS (
    SELECT *
    FROM {{ ref('bl_ally_brand_ally_slug_status') }}
    WHERE country_code = 'CO'
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
    FROM {{ ref('f_origination_events_co_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {% endif %}
    GROUP BY 1
)
,f_applications_marketplace_allies_product_policies_co_filtered_to_applicable AS (
    -- We get all the lists of product policies for marketplace applications (usually 3: ADDI_FINANCIA, ADDI_BNPN, ADDI_PAGO),
    -- then remaps the product policy value associated to the originations original product (one of these: FINANCIA_CO, BNPN_CO, PAGO_CO)
    -- After that we filter only the product policy applicable to the applications originations original product
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
,
f_applications_marketplace_suborders_co_weights_fix AS (
-- Due to issue with lack of precision (rounding to 2 decimals in backend events payloads) leading to weights not summing up 1
-- # addi_marketplace - SLACK THREAD: https://addico.slack.com/archives/C05RLMS96MV/p1712333758994209?thread_ts=1712327977.336739&cid=C05RLMS96MV
    SELECT
        *,
        CASE WHEN suborder_total_amount_without_discount > 0 THEN suborder_total_amount_without_discount ELSE suborder_total_amount END AS synthetic_suborder_total_amount,
        1                                      / (COUNT(suborder_id)                           OVER (PARTITION BY application_id)) AS synthetic_suborder_attribution_weight_by_number_of_allies,
        suborder_total_amount                  / (SUM(suborder_total_amount)                   OVER (PARTITION BY application_id)) AS synthetic_suborder_attribution_weight_by_total_amount,
        suborder_total_amount_without_discount / (SUM(suborder_total_amount_without_discount)  OVER (PARTITION BY application_id)) AS synthetic_suborder_attribution_weight_by_total_without_discount_amount,
        SUM(suborder_attribution_weight_by_number_of_allies) OVER (PARTITION BY application_id) AS sum_suborders_attribution_weight_by_number_of_allies,
        SUM(suborder_attribution_weight_by_total_amount) OVER (PARTITION BY application_id) AS sum_suborders_attribution_weight_by_total_amount,
        SUM(suborder_attribution_weight_by_total_without_discount_amount) OVER (PARTITION BY application_id) AS sum_suborders_attribution_weight_by_total_without_discount_amount
    FROM f_applications_marketplace_suborders_co
)
--- BOTH: BNPL + BNPN
SELECT
    mktp_so.custom_application_suborder_pairing_id,
    mktp_so.suborder_id,
    mktp_so.suborder_ally_slug,
    mktp_so.suborder_store_slug,
    mktp_so.application_id,
    o.loan_id,
    mktp_so.order_id,
    COALESCE(mktp_so.client_id, o.client_id, bf.client_id) AS client_id,
    COALESCE(mktp_so.ally_slug, o.ally_slug, a.ally_slug, bf.ally_slug) AS origination_ally_slug,
    o.origination_date,
    from_utc_timestamp(o.origination_date,"America/Bogota") AS origination_date_local,
    mktp_so.suborder_total_amount,
    mktp_so.suborder_total_amount_without_discount,
    mktp_so.synthetic_suborder_total_amount,
    mktp_so.suborder_shipping_amount,
    mktp_so.synthetic_suborder_attribution_weight_by_number_of_allies AS suborder_attribution_weight_by_number_of_allies,
    mktp_so.synthetic_suborder_attribution_weight_by_total_amount AS suborder_attribution_weight_by_total_amount,
    mktp_so.synthetic_suborder_attribution_weight_by_total_without_discount_amount AS suborder_attribution_weight_by_total_without_discount_amount,
    STRUCT(mktp_so.sum_suborders_attribution_weight_by_number_of_allies,
           mktp_so.sum_suborders_attribution_weight_by_total_amount,
           mktp_so.sum_suborders_attribution_weight_by_total_without_discount_amount) AS debug_original_weights,
    mktp_so.suborder_vtex_external_id_array,
    mktp_so.suborder_vtex_seller_id_array,
    mktp_so.suborder_marketplace_purchase_fee,
    (mktp_so.synthetic_suborder_total_amount * mktp_so.suborder_marketplace_purchase_fee) AS synthetic_suborder_marketplace_purchase_fee_amount,
    app_ftp.product_policy_type AS suborder_product_policy_type,
    app_ftp.product_policy_max_amount AS suborder_product_policy_max_amount,
    app_ftp.product_policy_cancellation_mdf AS suborder_cancellation_mdf,
    app_ftp.product_policy_fraud_mdf AS suborder_fraud_mdf,
    app_ftp.product_policy_origination_mdf AS suborder_origination_mdf,
    (mktp_so.synthetic_suborder_total_amount * app_ftp.product_policy_origination_mdf) AS synthetic_suborder_origination_mdf_amount,
    bs.ally_brand AS suborder_ally_brand,
    bs.ally_vertical AS suborder_ally_vertical,
    bs.ally_cluster AS suborder_ally_cluster,
    bs.status_ally_brand AS status_ally_brand_suborder,
    bs.status_ally_slug AS status_ally_slug_suborder,
    o.origination_approved_amount,
    o.origination_guarantee_rate,
    o.origination_term,
    COALESCE(a.channel, ac.application_channel, bf.channel) AS application_channel,
    CASE WHEN COALESCE(a.client_type,bf.client_type) ILIKE '%client%' OR a.journey_name ILIKE '%client%' OR a.custom_is_returning_client_legacy THEN 'CLIENT' ELSE 'PROSPECT' END AS client_type,
    COALESCE(a.journey_name,bf.journey_name) AS application_journey_name,
    COALESCE(mktp_so.product,ap.original_product) AS application_original_product,
    ap.processed_product AS application_processed_product,
    ap.synthetic_product_category AS application_synthetic_product_category,
    ap.synthetic_product_subcategory AS application_synthetic_product_subcategory,
    fr.price AS fx_rate,
    -- Data platform columns
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at

FROM       f_applications_marketplace_suborders_co_weights_fix AS mktp_so
LEFT JOIN  f_applications_marketplace_allies_product_policies_co_filtered_to_applicable AS app_ftp ON mktp_so.custom_application_suborder_pairing_id = app_ftp.custom_application_suborder_pairing_id
INNER JOIN f_originations_co AS o ON mktp_so.application_id = o.application_id
LEFT JOIN  f_applications_co AS a ON mktp_so.application_id = a.application_id
LEFT JOIN  applications_backfill_co AS bf ON mktp_so.application_id = bf.application_id
LEFT JOIN  bl_application_product_co AS ap ON ap.application_id=mktp_so.application_id
LEFT JOIN  bl_application_channel_co AS ac ON ac.application_id=mktp_so.application_id
LEFT JOIN  bl_ally_brand_ally_slug_status_co AS bs ON bs.ally_slug = mktp_so.suborder_ally_slug
LEFT JOIN  d_fx_rate AS fr ON 'CO' = fr.country_code AND fr.is_active = TRUE