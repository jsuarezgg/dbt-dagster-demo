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
target_applications_br AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_applications_br') }}
    WHERE CAST(last_event_ocurred_on_processed AS DATE) BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        last_event_ocurred_on_processed BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
{%- endif %}
f_applications_co AS (
    SELECT 		
		custom_platform_version,
		application_id,
		client_id,
	    application_date,
		client_type,
		channel,
		journey_name,
		requested_amount,
	    preapproval_expiration_date,
	  	preapproval_amount,
	  	custom_is_preapproval_completed
    FROM {{ source('silver_live', 'f_applications_co') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_origination_events_co_logs AS (
    SELECT 
		application_id,
		ocurred_on,
		client_id,
		channel,
		journey_name,
		client_type
    FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_co)
    {%- endif -%}
)
,
f_applications_br AS (
    SELECT 
		custom_platform_version,
		application_id,
		client_id,
	    application_date,
		client_type,
		channel,
		journey_name,
		requested_amount,
	    preapproval_expiration_date,
	  	preapproval_amount,
	  	custom_is_preapproval_completed
    FROM {{ ref('f_applications_br') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
f_origination_events_br_logs AS (
    SELECT 
		application_id,
		ocurred_on,
		client_id,
		channel,
		journey_name,
		client_type
    FROM {{ ref('f_origination_events_br_logs') }}
    {%- if is_incremental() %}
    WHERE application_id IN (SELECT application_id FROM target_applications_br)
    {%- endif -%}
)
,
applications_co_backfill AS (
	SELECT
		'CO' AS country_code,
		a.custom_platform_version,
		a.application_id,
		COALESCE(a.client_id,oe.client_id) AS client_id,
	    COALESCE(a.application_date, oe.proxy_application_date) AS application_date,
		COALESCE(a.client_type, oe.client_type) AS client_type,
		COALESCE(a.channel, oe.channel) AS channel,
		COALESCE(a.journey_name, oe.journey_name) AS journey_name,
		a.requested_amount,
	    a.preapproval_expiration_date::timestamp AS preapproval_expiration_date,
	  	a.preapproval_amount,
	  	a.custom_is_preapproval_completed
	FROM f_applications_co AS a
	LEFT JOIN (
		SELECT
			application_id,
			MIN(ocurred_on) AS proxy_application_date,
			FIRST_VALUE(client_id, TRUE) AS client_id,
			FIRST_VALUE(channel, TRUE) AS channel,
			FIRST_VALUE(journey_name, TRUE) AS journey_name,
			FIRST_VALUE(client_type, TRUE) AS client_type
	    FROM f_origination_events_co_logs
	    GROUP BY 1
    ) AS oe ON a.application_id = oe.application_id
)
,
applications_br_backfill AS (
	SELECT
		'BR' AS country_code,
		a.custom_platform_version,
		a.application_id,
		COALESCE(a.client_id,oe.client_id) AS client_id,
	    COALESCE(a.application_date, oe.proxy_application_date) AS application_date,
		COALESCE(a.client_type, oe.client_type) AS client_type,
		COALESCE(a.channel, oe.channel) AS channel,
		COALESCE(a.journey_name, oe.journey_name) AS journey_name,
		a.requested_amount,
	    a.preapproval_expiration_date::timestamp AS preapproval_expiration_date,
	  	a.preapproval_amount,
	  	a.custom_is_preapproval_completed
	FROM f_applications_br AS a
	LEFT JOIN (
		SELECT
			application_id,
			MIN(ocurred_on) AS proxy_application_date,
			FIRST_VALUE(client_id, TRUE) AS client_id,
			FIRST_VALUE(channel, TRUE) AS channel,
			FIRST_VALUE(journey_name, TRUE) AS journey_name,
			FIRST_VALUE(client_type, TRUE) AS client_type
	    FROM f_origination_events_br_logs
	    GROUP BY 1
    ) AS oe ON a.application_id = oe.application_id
)
,
applications_backfill AS (
	SELECT * FROM applications_co_backfill
	UNION ALL
	SELECT * FROM applications_br_backfill
)
,
application_preapproval_filtered AS (
	-- Only 1 approved preapproval application per day per client
	SELECT
		application_id,
		client_id,
	    application_date,
	    preapproval_expiration_date,
	  	preapproval_amount
	FROM applications_backfill
	WHERE channel ILIKE '%PRE%APPROVAL%' AND custom_is_preapproval_completed IS TRUE
	QUALIFY row_number() over(partition by client_id, application_date::date order by application_date) = 1
)
,
preapproval_flagged_applications AS (
	SELECT
		bf.application_id,
		TRUE AS flag_preapproval
		-- NAMED_STRUCT('relative_to_application_date', COLLECT_LIST(pf.application_date),
		-- 			 'relative_to_preapproval_expiration_date', COLLECT_LIST(pf.preapproval_expiration_date),
		-- 			 'relative_to_preapproval_amount', COLLECT_LIST(pf.preapproval_amount),
		-- 			 'relative_to_application_id', COLLECT_LIST(pf.application_id)) AS debug_relative_to_data
	FROM applications_backfill AS bf
	--- KEY CRITERIA ---
	INNER JOIN application_preapproval_filtered AS pf ON  bf.client_id = pf.client_id AND  bf.application_date > pf.application_date  and  bf.application_date <= pf.preapproval_expiration_date
	WHERE bf.requested_amount <= pf.preapproval_amount
        AND bf.client_type = 'PROSPECT'
        AND bf.journey_name NOT ILIKE '%preap%'
    --- END ------------
    GROUP BY 1,2
)
,
apps_no_preapp as (
  SELECT
    application_id,
    count(application_id) OVER (
      PARTITION BY client_id
      ORDER BY
        application_date :: date desc ROWS BETWEEN 30 PRECEDING
        AND CURRENT ROW
    ) as apps_no_preapp
  FROM {{ source('silver_live', 'f_applications_co') }}
  WHERE
    channel NOT like '%PRE%APPROVAL%'
    and product = "PAGO_CO"
    and custom_is_returning_client_legacy is not true
)

SELECT
	bf.country_code,
	bf.application_id,
	COALESCE(pfa.flag_preapproval,FALSE) AS is_using_preapproval_proxy,
	anp.apps_no_preapp,
	-- NAMED_STRUCT(
    --       'synthetic_application_date',bf.application_date,
    --       'custom_platform_version',bf.custom_platform_version,
    --       'client_id',bf.client_id,
    --       'client_type',bf.client_type,
    --       'channel',bf.channel,
    --       'journey_name',bf.journey_name,
    --       'requested_amount',bf.requested_amount,
    --       'preapproval_expiration_date',bf.preapproval_expiration_date,
    --       'preapproval_amount',bf.preapproval_amount,
    --       'custom_is_preapproval_completed',bf.custom_is_preapproval_completed
        --   'debug_relative_to_data',pfa.debug_relative_to_data) AS debug_data,
	NOW() AS ingested_at,
	to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM 	             applications_backfill AS bf
LEFT JOIN preapproval_flagged_applications AS pfa ON bf.application_id = pfa.application_id
LEFT JOIN apps_no_preapp anp ON bf.application_id = anp.application_id
