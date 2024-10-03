
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='event_id',
        incremental_strategy='merge',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--addi_prod.raw.braze_users_behaviors_subscription_globalstatechange
SELECT
    -- MANDATORY FIELDS
    CONCAT('braze.currents.',event_type) AS event_name_original,
    event_type AS event_name,
    id AS event_id,
    (`time`::TIMESTAMP) AS ocurred_on,
    (`time`::TIMESTAMP)::DATE AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- BRAZE NATIVE
    `time` AS time_unix,
    TRY_TO_TIMESTAMP(`date`, 'yyyy-MM-dd-HH') AS date_hour,
    external_user_id, --Ideally always client_id
    user_id,
    MD5(CONCAT(external_user_id, channel)) AS custom_external_user_channel_pairing_id,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    app_group_id,
    campaign_id,
    campaign_name,
    canvas_id,
    canvas_name,
    canvas_step_id,
    canvas_step_name,
    canvas_variation_id,
    canvas_variation_name,
    COALESCE(channel,CONCAT('_UNKNOWN_channel_',id)) AS channel,
    email_address,
    message_variation_id,
    state_change_source,
    subscription_status,
    timezone,
    -- CUSTOM ATTRIBUTES
    'GLOBAL_STATE_CHANGE' AS custom_behaviour_type
-- DBT SOURCE REFERENCE
FROM {{ source('raw', 'braze_users_behaviors_subscription_globalstatechange') }}
-- DBT INCREMENTAL SENTENCE
{% if is_incremental() %}
    WHERE `time`::TIMESTAMP BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_braze_em_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
 -- Filter only the first duplicated event_id (if ever happens)
QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY `time`::TIMESTAMP ASC) = 1