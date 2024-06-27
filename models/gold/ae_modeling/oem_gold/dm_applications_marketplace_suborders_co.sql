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
f_applications_marketplace_suborders_co AS (
    SELECT *
    FROM {{ ref('f_applications_marketplace_suborders_co') }}
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
d_fx_rate AS (
    SELECT *
    FROM {{ source('silver', 'd_fx_rate') }}
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

SELECT
    mktp_so.custom_application_suborder_pairing_id,
    mktp_so.suborder_id,
    mktp_so.suborder_ally_slug,
    mktp_so.suborder_store_slug,
    mktp_so.application_id,
    mktp_so.order_id,
    COALESCE(mktp_so.client_id, a.client_id, bf.client_id) AS client_id,
    COALESCE(mktp_so.ally_slug, a.ally_slug, bf.ally_slug) AS application_ally_slug,
    a.application_date as application_datetime,
    from_utc_timestamp(a.application_date,"America/Bogota") AS application_datetime_local,
    mktp_so.suborder_total_amount,
    mktp_so.suborder_total_amount_without_discount,
    CASE WHEN mktp_so.suborder_total_amount_without_discount > 0 THEN mktp_so.suborder_total_amount_without_discount ELSE mktp_so.suborder_total_amount END AS synthetic_suborder_total_amount,
    mktp_so.suborder_shipping_amount,
    mktp_so.suborder_vtex_external_id_array,
    mktp_so.suborder_vtex_seller_id_array,
    bs.ally_brand AS suborder_ally_brand,
    bs.ally_vertical AS suborder_ally_vertical,
    bs.ally_cluster AS suborder_ally_cluster,
    bs.status_ally_brand AS status_ally_brand_suborder,
    bs.status_ally_slug AS status_ally_slug_suborder,
    a.requested_amount AS application_requested_amount,
    a.requested_amount_without_discount AS application_requested_amount_without_discount,
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

FROM      f_applications_marketplace_suborders_co AS mktp_so
LEFT JOIN f_applications_co AS a ON mktp_so.application_id = a.application_id
LEFT JOIN applications_backfill_co AS bf ON mktp_so.application_id = bf.application_id
LEFT JOIN bl_application_product_co AS ap ON ap.application_id=mktp_so.application_id
LEFT JOIN bl_application_channel_co AS ac ON ac.application_id=mktp_so.application_id
LEFT JOIN bl_ally_brand_ally_slug_status_co AS bs ON bs.ally_slug = mktp_so.suborder_ally_slug
LEFT JOIN d_fx_rate AS fr ON 'CO' = fr.country_code AND fr.is_active = TRUE