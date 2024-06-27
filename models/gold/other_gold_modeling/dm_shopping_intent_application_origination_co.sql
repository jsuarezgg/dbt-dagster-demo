{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH shop_application AS (
    SELECT
        a.client_id,
        a.shopping_intent_id,
        a.application_id,
        application_datetime,
        application_datetime_local,
        requested_amount,
        requested_amount_without_discount,
        fr.price AS fx_rate,
        a.is_addishop_referral AS application_is_addishop_referral,
        a.is_addishop_referral_paid AS application_is_addishop_referral_paid,
        original_product,
        b.ally_slug AS application_ally_slug,
        b.ally_cluster AS application_ally_cluster,
        b.synthetic_channel AS application_ally_channel
    FROM {{ ref('bl_application_addi_shop_co') }} a
    LEFT JOIN {{ ref('dm_applications') }} b
        ON a.application_id = b.application_id
    LEFT JOIN silver.d_fx_rate fr 
        ON fr.country_code = 'CO' 
        AND fr.is_active IS true
)
, shop_origination AS (
    SELECT
        a.shopping_intent_id,
        a.application_id AS origination_application_id, 
        CASE WHEN a.application_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_origination_flag,
        b.loan_id,
        origination_date,
        origination_date_local,
        gmv,
        b.lead_gen_fee_rate,
        b.lead_gen_fee_amount,
        a.is_addishop_referral AS origination_is_addishop_referral,
        a.is_addishop_referral_paid AS origination_is_addishop_referral_paid
    FROM {{ ref('bl_application_addi_shop_co') }} a
    LEFT JOIN {{ ref('dm_originations') }} b
        ON a.application_id = b.application_id
    WHERE 1=1 
        AND a.country_code = 'CO'
        AND a.is_addishop_referral IS TRUE
)
, application_origination AS (
SELECT
    sa.client_id,
    COALESCE(so.shopping_intent_id, sa.shopping_intent_id) AS shopping_intent_id,
    sa.application_id,
    sa.application_datetime,
    sa.application_datetime_local,
    sa.application_ally_slug,
    sa.application_ally_cluster,
    sa.application_ally_channel,
    sa.requested_amount,
    sa.requested_amount_without_discount,
    sa.fx_rate,
    COALESCE(so.origination_is_addishop_referral, sa.application_is_addishop_referral) AS application_is_addishop_referral,
    COALESCE(so.origination_is_addishop_referral_paid, sa.application_is_addishop_referral_paid) AS application_is_addishop_referral_paid,
    sa.original_product,
    so.is_origination_flag,
    so.origination_application_id,
    so.loan_id,
    so.origination_date,
    so.origination_date_local,
    so.gmv,
    so.lead_gen_fee_rate,
    so.lead_gen_fee_amount,
    so.origination_is_addishop_referral,
    so.origination_is_addishop_referral_paid
FROM shop_application sa
FULL JOIN shop_origination so
    ON sa.application_id = so.origination_application_id
)
, amplitude_device_info AS (
    SELECT
        user_id,
        device_id,
        platform,
        device_type,
        ROW_NUMBER() OVER (PARTITION BY device_id ORDER BY COUNT(*) DESC) as rank
    FROM {{ ref('dm_amplitude') }}
    GROUP BY ALL
    QUALIFY rank = 1
)
, amplitude_user_device AS (
    SELECT
        user_id,
        platform,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC) as ranked
    FROM amplitude_device_info
    GROUP BY ALL
    QUALIFY ranked = 1
)
SELECT
    si.shopping_intent_id,
    si.shopping_intent_timestamp,
    from_utc_timestamp(si.shopping_intent_timestamp,'America/Bogota') AS shopping_intent_timestamp_local,
    si.ally_slug,
    abs.ally_brand,
    abs.ally_vertical,
    abs.ally_cluster,
    si.campaign_id,
    si.channel AS shop_channel,
    CASE WHEN si.client_id = '' THEN NULL ELSE si.client_id END AS client_id,
    si.device_id,
    COALESCE(adi.platform, aud.platform) AS platform,
    adi.device_type,
    si.screen,
    si.component,
    si.category,
    si.subcategory,
    sa.client_id AS application_client_id,
    sa.application_id,
    sa.application_datetime,
    sa.application_datetime_local,
    sa.application_ally_slug,
    sa.application_ally_cluster,
    sa.application_ally_channel,
    sa.requested_amount,
    sa.requested_amount_without_discount,
    sa.fx_rate,
    sa.application_is_addishop_referral,
    sa.application_is_addishop_referral_paid,
    sa.original_product,
    sa.is_origination_flag,
    sa.origination_application_id,
    sa.loan_id,
    sa.origination_date,
    sa.origination_date_local,
    sa.gmv,
    sa.lead_gen_fee_rate,
    sa.lead_gen_fee_amount,
    sa.origination_is_addishop_referral,
    sa.origination_is_addishop_referral_paid
FROM {{ ref('f_marketplace_shopping_intents_co') }} si
LEFT JOIN {{ ref('bl_ally_brand_ally_slug_status') }} abs
    ON abs.country_code = 'CO'
    AND si.ally_slug = abs.ally_slug
LEFT JOIN application_origination sa
    ON sa.shopping_intent_id = si.shopping_intent_id
LEFT JOIN amplitude_device_info adi
    ON si.device_id = adi.device_id
LEFT JOIN amplitude_user_device aud
    ON si.client_id = aud.user_id