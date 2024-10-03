{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
{% set snp_table_exists = unity_catalog_table_exists('gold','snp_braze_purchase_events') %}
WITH
dm_originations AS (
    SELECT *
    FROM {{ ref('dm_originations') }}
)
,
agg_braze_proxy_user_status_co AS (
    SELECT *
    FROM {{ ref('agg_braze_proxy_user_status_co') }}
)
,
{% if snp_table_exists %}
tracked_applications_snp_braze_purchase_events AS (
    SELECT
        DISTINCT application_id
    FROM {{ source('gold', 'snp_braze_purchase_events') }} -- Hard-coded reference to prevent dependency circuit-break
)
,
{% endif %}
purchases_baseline_co AS (
    SELECT
        *, --To avoid redundancy, only new fields
        -- Hard filter components
        (gmv_is_not_null AND synthetic_product_category_is_not_null AND synthetic_channel_is_mapped) AS ae_complies_basic_criteria, -- Flags as to the gold data processing is completed
        CASE
             WHEN client_is_on_braze_proxy AND (NOW() >= (client_last_update_braze_proxy_timestamp + INTERVAL '2' HOUR)) --2 Hours as slack time to make sure the proxy translates in a braze sync of both attributes and deletion
                THEN within_eligible_time_window AND (NOT application_is_tracked_already)
            ELSE FALSE
        END AS to_be_tracked_braze_snapshots
    FROM
    (-- KEY: Tracking data for originations (purchase events). Some flags are used to track if a row is 'ready'
        SELECT
            -- A. PK Field
            a.application_id,
            -- B. Other fields: some of these are expected to be tracked others are brought to validate
            a.loan_id,
            a.client_id,
            a.ally_slug,
            a.suborders_ally_slug_array,
            a.origination_date AS origination_datetime_utc,
            LOWER(a.ally_vertical) AS ally_vertical_lowercase,
            'USD' AS currency,
            (a.gmv / a.fx_rate) AS gmv_usd,
            a.application_channel,
            a.synthetic_channel,
            (a.debug_for_braze_proxy_total_revenue / a.fx_rate) AS revenue_proxy_usd,
            a.synthetic_product_category,
            COALESCE(a.is_addishop_referral, FALSE) AS is_addishop_referral_with_default,
            a.fx_rate,
            -- C. Flags to consider a row as being post-processed successfully = ready to send
            a.gmv IS NOT NULL AS gmv_is_not_null,
            a.synthetic_product_category IS NOT NULL AS synthetic_product_category_is_not_null,
            COALESCE(a.synthetic_channel != '_PENDING_MAPPING_',FALSE) AS synthetic_channel_is_mapped,
            COALESCE(bp.is_on_braze_proxy, FALSE) AS client_is_on_braze_proxy,
            bp.braze_last_update_proxy_timestamp AS client_last_update_braze_proxy_timestamp,
            COALESCE(a.origination_date >= (NOW() - INTERVAL '32' HOUR),FALSE) within_eligible_time_window, -- Safe slack time to make sure we wait for the next day user creation ( plus few hours)
            {% if snp_table_exists %}ta.application_id IS NOT NULL{% else %}FALSE{% endif %} AS application_is_tracked_already
        FROM dm_originations AS a
        {% if snp_table_exists %}LEFT JOIN tracked_applications_snp_braze_purchase_events AS ta ON a.application_id = ta.application_id{% endif %}
        LEFT JOIN agg_braze_proxy_user_status_co AS bp ON a.client_id = bp.client_id
        WHERE 1=1
            AND a.country_code = 'CO'
            AND a.origination_date >= '2024-08-21 15:00:00'  -- Hard-coded timestamp of when to start measuring originations; it was decided we only want to upload data moving forward; needs to be updated when is about to be productive
    )
)

SELECT
    -- Full context: Braze Customer Engagement Platform - Data Scope: https://www.notion.so/addico/Braze-Customer-Engagement-Platform-Data-Scope-7502aa4eaa954b47a6e84f81b026e23d?pvs=4
    b.application_id,
    -- A. Snapshot hard-filter flags
    b.ae_complies_basic_criteria,
    b.to_be_tracked_braze_snapshots,
    -- B. Tracked values & fields to validate results (not all are tracked)
    b.loan_id,
    b.client_id,
    b.ally_slug,
    b.suborders_ally_slug_array,
    b.origination_datetime_utc,
    b.ally_vertical_lowercase,
    b.currency,
    b.gmv_usd,
    b.application_channel,
    b.synthetic_channel,
    b.revenue_proxy_usd,
    b.synthetic_product_category,
    b.is_addishop_referral_with_default,
    b.fx_rate,
    -- C. Debugging the key flags above
    STRUCT(b.gmv_is_not_null, b.synthetic_product_category_is_not_null, b.synthetic_channel_is_mapped) AS debug_ae_complies_basic_criteria,
    STRUCT(b.client_is_on_braze_proxy, b.client_last_update_braze_proxy_timestamp,  b.application_is_tracked_already, b.within_eligible_time_window) AS debug_to_be_tracked_braze_snapshots,
    -- D. DATA PLATFORM DATA
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM purchases_baseline_co AS b