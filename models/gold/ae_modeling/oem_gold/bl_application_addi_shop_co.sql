{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


WITH
{%- if is_incremental() %}
target_applications_co AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_co') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
),

target_applications_br AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_br') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
),

target_shopping_intents_co AS (
    SELECT DISTINCT shopping_intent_id
    FROM {{ ref('f_marketplace_transaction_attributable_co') }}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
),

target_shopping_intents_br AS (
    SELECT DISTINCT shopping_intent_id
    FROM {{ ref('f_marketplace_transaction_attributable_br') }}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
),

{%- endif %}
f_marketplace_transaction_attributable_co AS (
    SELECT *
    FROM {{ ref('f_marketplace_transaction_attributable_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
),

f_marketplace_shopping_intents_co AS (
    SELECT *
    FROM {{ ref('f_marketplace_shopping_intents_co') }}
    {%- if is_incremental() %}
    WHERE shopping_intent_id IN (SELECT shopping_intent_id FROM target_shopping_intents_co)
    {%- endif -%}
),

f_marketplace_transaction_attributable_br AS (
    SELECT *
    FROM {{ ref('f_marketplace_transaction_attributable_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
),

f_marketplace_shopping_intents_br AS (
    SELECT *
    FROM {{ ref('f_marketplace_shopping_intents_br') }}
    {%- if is_incremental() %}
    WHERE shopping_intent_id IN (SELECT shopping_intent_id FROM target_shopping_intents_br)
    {%- endif -%}
),

f_originations_bnpl_co AS (
    SELECT *
    FROM {{ ref('f_originations_bnpl_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
),

f_originations_bnpn_co AS (
    SELECT *
    FROM {{ ref('f_originations_bnpn_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
),

f_originations_bnpl_br AS (
    SELECT *
    FROM {{ ref('f_originations_bnpl_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
),

f_originations_bnpn_br AS (
    SELECT *
    FROM {{ ref('f_originations_bnpn_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
),

addishop_attributable_co AS (
  SELECT
    m.application_id,
    m.shopping_intent_id,
    si.channel AS addishop_channel,
    si.campaign_id,
    COALESCE(m.device_id,si.device_id) AS device_id,
    m.used_grouped_config
  FROM f_marketplace_transaction_attributable_co AS m
  LEFT JOIN  f_marketplace_shopping_intents_co AS si ON m.shopping_intent_id = si.shopping_intent_id
), 

addishop_attributable_br AS (
  SELECT
    m.application_id,
    m.shopping_intent_id,
    si.channel AS addishop_channel,
    si.campaign_id,
    COALESCE(m.device_id,si.device_id) AS device_id
  FROM f_marketplace_transaction_attributable_br AS m
  LEFT JOIN  f_marketplace_shopping_intents_br AS si ON m.shopping_intent_id = si.shopping_intent_id
),

dm_addishop_paying_allies_co AS (
  SELECT
      ally_slug,
      start_date::timestamp AS start_date,
      end_date::timestamp AS end_date,
      lead_gen_fee::double AS lead_gen_fee,
        grouped_allies,
      current_status
  FROM {{ ref('dm_addishop_paying_allies_co') }}
),

originations_br AS (
  SELECT
    application_id,
    client_id,
    loan_id,
    ally_slug,
    origination_date,
    'BNPL' as transaction_type
  FROM f_originations_bnpl_br
  UNION ALL
  SELECT
    application_id,
    client_id,
    NULL AS loan_id,
    ally_slug,
    origination_date,
    'BNPN' as transaction_type
  FROM f_originations_bnpn_br
),

originations_co AS (
  SELECT
    application_id,
    client_id,
    loan_id,
    ally_slug,
    origination_date,
    'BNPL' as transaction_type
  FROM f_originations_bnpl_co
  UNION ALL
  SELECT
    application_id,
    client_id,
    NULL AS loan_id,
    ally_slug,
    last_event_ocurred_on_processed as origination_date,
    'BNPN' as transaction_type
  FROM f_originations_bnpn_co
),

addishop_flags_origination_co AS (
	SELECT
		'CO' AS country_code,
		o.application_id,
		o.loan_id,
    o.client_id,
		o.ally_slug,
		o.origination_date,
		FROM_UTC_TIMESTAMP(o.origination_date,'America/Bogota') AS origination_date_local,
        shop.shopping_intent_id,
		shop.addishop_channel,
		shop.campaign_id,
		shop.device_id,
        shop.used_grouped_config,
		pa_co.start_date AS addi_shop_ally_period_opt_in_date,
		pa_co.end_date AS addi_shop_ally_period_opt_out_date,
        pa_co.grouped_allies,
		CASE WHEN shop.application_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_addishop_referral,
		CASE WHEN shop.application_id IS NOT NULL AND pa_co.ally_slug IS NOT NULL THEN TRUE ELSE FALSE END AS is_addishop_referral_paid,
		CASE WHEN shop.application_id IS NOT NULL AND pa_co.ally_slug IS NOT NULL THEN pa_co.lead_gen_fee ELSE NULL END AS lead_gen_fee_rate,
		o.transaction_type
	FROM      originations_co AS o
	LEFT JOIN addishop_attributable_co      AS shop  ON o.application_id = shop.application_id
	LEFT JOIN dm_addishop_paying_allies_co  AS pa_co ON (o.ally_slug = pa_co.ally_slug OR array_contains(pa_co.grouped_allies, o.ally_slug) IS TRUE)
		                                                AND o.origination_date >= pa_co.start_date
		                                                AND o.origination_date <= pa_co.end_date
),

addishop_flags_origination_br AS (
	SELECT
		'BR' AS country_code,
		o.application_id,
		o.loan_id,
    o.client_id,
		o.ally_slug,
		o.origination_date,
		FROM_UTC_TIMESTAMP(o.origination_date,'America/Sao_Paulo') AS origination_date_local,
        shop.shopping_intent_id,
		shop.addishop_channel,
		shop.campaign_id,
		shop.device_id,
        NULL AS used_grouped_config,
		NULL AS addi_shop_ally_period_opt_in_date,
		NULL AS addi_shop_ally_period_opt_out_date,
        NULL AS grouped_allies,
		CASE WHEN shop.application_id IS NOT NULL THEN TRUE ELSE FALSE END AS is_addishop_referral,
		NULL AS is_addishop_referral_paid,
		NULL AS lead_gen_fee_rate,
		o.transaction_type
	FROM      originations_br AS o
	LEFT JOIN addishop_attributable_br      AS shop ON o.application_id = shop.application_id
),

addishop_flags_origination AS (
  SELECT * FROM addishop_flags_origination_co
  UNION ALL
  SELECT * FROM addishop_flags_origination_br
),

originations_final_table as (
    SELECT
  country_code,
  application_id,
  client_id,
  loan_id,
  ally_slug,
  is_addishop_referral,
  is_addishop_referral_paid,
    shopping_intent_id,
    used_grouped_config,
  lead_gen_fee_rate,
  addishop_channel,
  addi_shop_ally_period_opt_in_date,
  addi_shop_ally_period_opt_out_date,
  NAMED_STRUCT(
                'ally_slug',ally_slug,
              'transaction_type',transaction_type,
                'origination_date',origination_date,
                'origination_date_local',origination_date_local,
                'campaign_id',campaign_id,
                'device_id',device_id,
                'grouped_allies',grouped_allies) AS debug_data
FROM addishop_flags_origination
),

apps_shopping_intents AS (
SELECT 
    client_id,
    shopping_intent_id,
    ally_slug,
    ocurred_on_date,
    shopping_intent_timestamp,
    channel
FROM f_marketplace_shopping_intents_co
WHERE 1=1
),

apps_interest_applications_co as (
    SELECT
      application_id,
      client_id,
      application_date,
      ally_slug,
      channel
    FROM {{ ref('f_applications_co') }} AS io_co
    WHERE 1=1
      AND lower(io_co.journey_name) NOT LIKE '%preapproval%' -- Preapproval journeys are not included in this table scope.
      AND io_co.application_date < '2023-10-12'
), 

apps_addishop_application_attribution AS (
SELECT 
  io.*,
  si.shopping_intent_id,
  si.shopping_intent_timestamp AS last_shopping_intention,
  si.channel AS addishop_channel,
  row_number() OVER (PARTITION BY application_id ORDER BY shopping_intent_timestamp DESC) AS shopping_intent_order
FROM apps_interest_applications_co AS io
LEFT JOIN apps_shopping_intents AS si 
  ON io.client_id = si.client_id
  AND io.ally_slug = si.ally_slug
  AND si.shopping_intent_timestamp < io.application_date -- APP event timestamp BEFORE origination timestamp
  AND si.shopping_intent_timestamp >= (io.application_date - interval 2 day) -- APP event IN INTERVAL 
WHERE 1=1
QUALIFY shopping_intent_order = 1
), 

apps_legacy_application_shop_attributed AS (
SELECT 
    application_date,
    aa.client_id,
    aa.application_id,
    aa.ally_slug,
    shopping_intent_id,
    last_shopping_intention,
    addishop_channel,
    CASE WHEN last_shopping_intention IS NOT NULL THEN TRUE ELSE FALSE END AS is_addishop_referral,
    CASE WHEN last_shopping_intention IS NOT NULL AND apa.ally_slug IS NOT NULL THEN TRUE ELSE FALSE END AS is_addishop_referral_paid,
    apa.start_date AS addi_shop_ally_period_opt_in_date,
    apa.end_date AS addi_shop_ally_period_opt_out_date,
    apa.lead_gen_fee AS lead_gen_fee_rate,
    NOW() AS ingested_at
FROM apps_addishop_application_attribution aa
LEFT JOIN {{ ref('dm_addishop_paying_allies_co') }} apa
  ON (aa.ally_slug = apa.ally_slug OR array_contains(apa.grouped_allies, aa.ally_slug) IS TRUE)
  AND aa.application_date >= apa.start_date
  AND aa.application_date <= apa.end_date
),

apps_new_application_shop_attributed AS (
  SELECT
    last_event_ocurred_on_processed AS application_date,
    o.client_id,
    o.application_id,
    o.ally_slug,  
    si.shopping_intent_id,
    shopping_intent_timestamp AS last_shopping_intention, 
    si.channel AS addishop_channel, 
    order_shopping_intent_attributable AS is_addishop_referral,
    CASE WHEN order_shopping_intent_attributable IS TRUE AND apa.ally_slug IS NOT NULL THEN TRUE ELSE FALSE END AS is_addishop_referral_paid,
    apa.start_date AS addi_shop_ally_period_opt_in_date,
    apa.end_date AS addi_shop_ally_period_opt_out_date,
    apa.lead_gen_fee AS lead_gen_fee_rate,
    NOW() AS ingested_at
  FROM {{ ref('f_applications_order_details_v2_co') }} o
  LEFT JOIN apps_shopping_intents si
    ON o.order_shopping_intent_id = si.shopping_intent_id
  LEFT JOIN {{ ref('dm_addishop_paying_allies_co') }} apa
    ON (o.ally_slug = apa.ally_slug OR array_contains(apa.grouped_allies, o.ally_slug) IS TRUE)
    AND o.last_event_ocurred_on_processed >= apa.start_date
    AND o.last_event_ocurred_on_processed <= apa.end_date
    AND order_shopping_intent_attributable IS TRUE
  WHERE 1=1
    AND order_shopping_intent_attributable IS NOT NULL
    AND o.ocurred_on_date > '2023-10-11'
),

apps_final_table as (
SELECT
  * 
FROM apps_new_application_shop_attributed

UNION ALL

SELECT 
  * 
FROM apps_legacy_application_shop_attributed
)

SELECT 
  COALESCE(oft.country_code,'CO') as country_code,
  COALESCE(oft.application_id,aft.application_id) as application_id,
  COALESCE(oft.client_id,aft.client_id) as client_id,
  oft.loan_id,
  COALESCE(oft.ally_slug,aft.ally_slug) as ally_slug,
  COALESCE(oft.is_addishop_referral,aft.is_addishop_referral) as is_addishop_referral,
  COALESCE(oft.is_addishop_referral_paid,aft.is_addishop_referral_paid) as is_addishop_referral_paid,
  COALESCE(oft.shopping_intent_id,aft.shopping_intent_id) as shopping_intent_id,
  oft.used_grouped_config,
  COALESCE(oft.lead_gen_fee_rate,aft.lead_gen_fee_rate) as lead_gen_fee_rate,
  COALESCE(oft.addishop_channel,aft.addishop_channel) as addishop_channel,
  COALESCE(oft.addi_shop_ally_period_opt_in_date,aft.addi_shop_ally_period_opt_in_date) as addi_shop_ally_period_opt_in_date,
  COALESCE(oft.addi_shop_ally_period_opt_out_date,aft.addi_shop_ally_period_opt_out_date) as addi_shop_ally_period_opt_out_date,
  COALESCE(oft.debug_data,null) as debug_data,
  NOW() AS ingested_at,
  to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM originations_final_table oft
FULL JOIN apps_final_table aft ON oft.application_id=aft.application_id