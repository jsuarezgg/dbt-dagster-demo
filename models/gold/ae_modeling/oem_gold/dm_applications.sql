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
    FROM {{ ref('f_applications_co') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
target_applications_br AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_br') }}
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
f_applications_br AS (
    SELECT *
    FROM {{ ref('f_applications_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_applications_declination_data_co AS (
    SELECT *
    FROM {{ ref('f_applications_declination_data_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_declination_data_br AS (
    SELECT *
    FROM {{ ref('f_applications_declination_data_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
d_fx_rate AS (
    SELECT *
    FROM {{ source('silver', 'd_fx_rate') }}
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
,
applications_backfill_br AS (
    SELECT
        application_id,
        FIRST_VALUE(ally_slug,TRUE) AS ally_slug,
        FIRST_VALUE(channel,TRUE) AS channel,
        FIRST_VALUE(client_id,TRUE) AS client_id,
        FIRST_VALUE(client_type,TRUE) AS client_type,
        FIRST_VALUE(journey_name,TRUE) AS journey_name
    FROM {{ ref('f_origination_events_br_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {% endif %}
    GROUP BY 1
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
bl_application_product_br AS (
    SELECT *
    FROM {{ ref('bl_application_product_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
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
bl_application_channel_br AS (
    SELECT *
    FROM {{ ref('bl_application_channel') }}
    WHERE country_code = 'BR'
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
dm_originations_last_event_priority_on_termination_br AS (
    SELECT *
    FROM {{ ref('dm_originations_last_event_priority_on_termination_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
dm_originations_last_event_priority_on_termination_co AS (
    SELECT *
    FROM {{ ref('dm_originations_last_event_priority_on_termination_co') }}
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
bl_ally_brand_ally_slug_status_br AS (
    SELECT *
    FROM {{ ref('bl_ally_brand_ally_slug_status') }}
    WHERE country_code = 'BR'
)
,
bl_application_preapproval_proxy_co AS (
    SELECT *
    FROM {{ ref('bl_application_preapproval_proxy') }}
    WHERE country_code = 'CO'
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
bl_application_preapproval_proxy_br AS (
    SELECT *
    FROM {{ ref('bl_application_preapproval_proxy') }}
    WHERE country_code = 'BR'
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
bl_application_addi_shop_co AS (
    SELECT *
    FROM {{ ref('bl_application_addi_shop_co') }}
    WHERE 1=1
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
device_information_updated AS (
    SELECT *
    FROM {{ ref('f_device_information_stage_co') }}
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%})
,

f_applications_order_details_v2_co AS (
    SELECT *
    FROM {{ ref('f_applications_order_details_v2_co') }}
    {%- if is_incremental() %}
    AND application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%})
,

application_dm AS (
SELECT
    "CO" as country_code
    ,a.application_id
    ,COALESCE(a.client_id,bf.client_id) AS client_id
    ,a.application_date as application_datetime
    ,from_utc_timestamp(a.application_date,"America/Bogota") AS application_datetime_local
    ,from_utc_timestamp(a.application_date,"America/Bogota")::date AS application_date_local
    ,COALESCE(a.channel,a.application_channel_legacy, ac.application_channel, bf.channel) AS application_channel
    ,ac.synthetic_channel
    ,COALESCE(a.ally_slug,bf.ally_slug) AS ally_slug
    ,CASE WHEN a.suborders_ally_slug_array IS NOT NULL THEN IF(SIZE(a.suborders_ally_slug_array)>0, SORT_ARRAY(a.suborders_ally_slug_array),NULL) END AS suborders_ally_slug_array
    ,a.store_slug
    ,bs.ally_brand
    ,bs.ally_vertical
    ,bs.ally_cluster
    ,a.order_id
    ,CASE WHEN COALESCE(a.client_type,bf.client_type) ILIKE '%client%' OR a.journey_name ILIKE '%client%' OR a.custom_is_returning_client_legacy THEN 'CLIENT' ELSE 'PROSPECT' END AS client_type
    ,a.custom_platform_version
    ,COALESCE(a.journey_name,bf.journey_name) AS journey_name
    ,ap.original_product
    ,ap.processed_product
    ,ap.synthetic_product_category
    ,ap.synthetic_product_subcategory
    ,a.requested_amount
    ,a.requested_amount_without_discount
    ,CASE WHEN a.requested_amount_without_discount >0 THEN a.requested_amount_without_discount ELSE a.requested_amount END AS synthetic_requested_amount
    ,le.last_event_type AS last_event_type_prime
    ,le.last_journey_stage_name AS last_journey_stage_name_prime
    ,le.last_event_name AS last_event_name_prime
    ,le.last_event_id AS last_event_id_prime
    ,a.campaign_id
    ,a.store_user_id
    ,bs.status_ally_brand
    ,bs.status_ally_slug
    ,pp.is_using_preapproval_proxy
    ,a.preapproval_amount
    ,a.preapproval_expiration_date
    ,a.custom_is_preapproval_completed
    ,NULL as custom_is_bnpn_branched
    ,CASE WHEN a.journey_name ILIKE '%checkpoint%' OR a.custom_is_checkpoint_application_legacy THEN TRUE ELSE FALSE END AS custom_is_checkpoint_application
    ,CASE WHEN a.channel ILIKE '%pre_approval%' OR a.application_channel_legacy='PREAPPROVAL' THEN TRUE ELSE FALSE END AS custom_is_preapproval_application
    ,a.custom_is_santander_branched
    ,d.declination_reason
    ,d.declination_comments_redacted
    ,diu.device_id
    --,aod.order_shopping_intent_attributable
    ,apas.is_addishop_referral
    ,apas.addishop_channel
    ,apas.is_addishop_referral_paid
    ,apas.addi_shop_ally_period_opt_in_date AS addishop_opt_in_date
    ,apas.addi_shop_ally_period_opt_out_date AS addishop_opt_out_date
    ,apas.lead_gen_fee_rate
    ,fr.price AS fx_rate
--  ,a.client_is_transactional_based
--  ,a.store_user_name
--  ,a.application_channel_legacy
--  ,NULL as client_checkout_type
--  ,a.custom_is_checkpoint_application_legacy
--  ,a.custom_is_privacy_policy_accepted
--  ,a.custom_is_returning_client_legacy

FROM      f_applications_co AS a
LEFT JOIN applications_backfill_co AS bf ON a.application_id = bf.application_id
LEFT JOIN dm_originations_last_event_priority_on_termination_co AS le ON le.application_id = a.application_id
LEFT JOIN f_applications_declination_data_co AS d ON a.application_id = d.application_id
LEFT JOIN bl_application_product_co AS ap ON ap.application_id=a.application_id
LEFT JOIN bl_application_channel_co AS ac ON ac.application_id=a.application_id
LEFT JOIN bl_ally_brand_ally_slug_status_co AS bs ON a.ally_slug = bs.ally_slug
LEFT JOIN bl_application_preapproval_proxy_co as pp ON pp.application_id=a.application_id
LEFT JOIN device_information_updated as diu ON diu.application_id=a.application_id
LEFT JOIN f_applications_order_details_v2_co as aod ON aod.application_id = a.application_id
LEFT JOIN bl_application_addi_shop_co as apas ON apas.application_id = a.application_id
LEFT JOIN d_fx_rate AS fr ON 'CO' = fr.country_code AND fr.is_active = TRUE

UNION ALL

SELECT
    "BR" as country_code
    ,a.application_id
    ,COALESCE(a.client_id,bf.client_id) AS client_id
    ,a.application_date as application_datetime
    ,from_utc_timestamp(a.application_date,"America/Sao_Paulo") AS application_datetime_local
    ,from_utc_timestamp(a.application_date,"America/Sao_Paulo")::date AS application_date_local
    ,COALESCE(a.channel,a.application_channel_legacy, ac.application_channel, bf.channel) AS application_channel
    ,ac.synthetic_channel
    ,COALESCE(a.ally_slug,bf.ally_slug) AS ally_slug
    ,NULL AS suborders_ally_slug_array
    ,a.store_slug
    ,bs.ally_brand
    ,bs.ally_vertical
    ,bs.ally_cluster
    ,a.order_id
    ,CASE WHEN COALESCE(a.client_type,bf.client_type) ILIKE '%client%' OR a.journey_name ILIKE '%client%' OR a.custom_is_returning_client_legacy THEN 'CLIENT' ELSE 'PROSPECT' END AS client_type
    ,a.custom_platform_version
    ,COALESCE(a.journey_name,bf.journey_name) AS journey_name
    ,ap.original_product
    ,ap.processed_product
    ,ap.synthetic_product_category
    ,ap.synthetic_product_subcategory
    ,a.requested_amount
    ,a.requested_amount_without_discount
    ,CASE WHEN a.requested_amount_without_discount >0 THEN a.requested_amount_without_discount ELSE a.requested_amount END AS synthetic_requested_amount
    ,le.last_event_type AS last_event_type_prime
    ,le.last_journey_stage_name AS last_journey_stage_name_prime
    ,le.last_event_name AS last_event_name_prime
    ,le.last_event_id AS last_event_id_prime
    ,a.campaign_id
    ,a.store_user_id
    ,bs.status_ally_brand
    ,bs.status_ally_slug
    ,pp.is_using_preapproval_proxy
    ,a.preapproval_amount
    ,a.preapproval_expiration_date
    ,a.custom_is_preapproval_completed
    ,a.custom_is_bnpn_branched
    ,CASE WHEN a.journey_name ILIKE '%checkpoint%' OR a.custom_is_checkpoint_application_legacy THEN TRUE ELSE FALSE END AS custom_is_checkpoint_application
    ,CASE WHEN a.channel ILIKE '%pre_approval%' OR a.application_channel_legacy='PREAPPROVAL' THEN TRUE ELSE FALSE END AS custom_is_preapproval_application
    ,NULL as custom_is_santander_branched
    ,d.declination_reason
    ,d.declination_comments_redacted
    ,diu.device_id
    ,FALSE AS is_addishop_referral
    ,NULL AS addishop_channel
    ,FALSE AS is_addishop_referral_paid
    ,NULL AS addishop_opt_in_date
    ,NULL AS addishop_opt_out_date
    ,NULL AS lead_gen_fee_rate
    ,fr.price AS fx_rate
--  ,NULL AS client_is_transactional_based
--,null as store_user_name
--,a.application_channel_legacy
--,a.client_checkout_type
--,a.custom_is_checkpoint_application_legacy
--,a.custom_is_privacy_policy_accepted
--,a.custom_is_returning_client_legacy
FROM      f_applications_br AS a
LEFT JOIN applications_backfill_br AS bf ON a.application_id = bf.application_id
LEFT JOIN dm_originations_last_event_priority_on_termination_br AS le ON le.application_id = a.application_id
LEFT JOIN f_applications_declination_data_br AS d ON a.application_id = d.application_id
LEFT JOIN bl_application_product_br AS ap ON ap.application_id=a.application_id
LEFT JOIN bl_application_channel_br AS ac ON ac.application_id=a.application_id
LEFT JOIN bl_ally_brand_ally_slug_status_br AS bs ON a.ally_slug = bs.ally_slug
LEFT JOIN bl_application_preapproval_proxy_br as pp ON pp.application_id=a.application_id
LEFT JOIN device_information_updated as diu ON diu.application_id=a.application_id
LEFT JOIN d_fx_rate AS fr ON 'BR' = fr.country_code AND fr.is_active = TRUE
)

SELECT 
    *
    ,NOW() AS ingested_at
    ,to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM application_dm
