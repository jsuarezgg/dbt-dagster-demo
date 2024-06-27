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
target_applications AS (
    SELECT DISTINCT application_id
    FROM {{ ref('f_origination_events_br_logs') }}
    WHERE ocurred_on_date BETWEEN (to_date('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date('{{ var("end_date","placeholder_end_date") }}') AND
        ocurred_on BETWEEN (to_timestamp('{{ var("start_date","placeholder_prev_exec_date") }}'- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp('{{ var("end_date","placeholder_end_date") }}')
)
,
{%- endif %}
f_origination_termination_events_br_logs AS (
    SELECT *
    FROM {{ ref('f_origination_termination_events_br_logs') }}
    {%- if is_incremental() %}
    WHERE  application_id IN (SELECT application_id FROM target_applications)
    {%- endif -%}
)
,
f_origination_events_br_logs AS (
    SELECT *
    FROM {{ ref('f_origination_events_br_logs') }}
    {%- if is_incremental() %}
    WHERE  application_id IN (SELECT application_id FROM target_applications)
    {%- endif -%}
)
,
f_origination_events_br AS (
    SELECT *
    FROM {{ ref('f_origination_events_br') }}
    {%- if is_incremental() %}
    WHERE  application_id IN (SELECT application_id FROM target_applications)
    {%- endif -%}
)
,
last_termination_global_event_br AS (
    -- ONLY USING GLOBAL TERMINATION EVENTS (LAST NON-NULL VALUE TECHNIQUE TO AVOID WINDOW_FUNCTIONS)
    SELECT application_id,
           'termination_only' AS debug_termination_source,
           element_at(array_sort(array_agg(CASE WHEN event_type IS NOT NULL THEN struct(ocurred_on, event_type) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).event_type AS event_type,
           element_at(array_sort(array_agg(CASE WHEN journey_stage_name IS NOT NULL THEN struct(ocurred_on, journey_stage_name) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).journey_stage_name AS journey_stage_name,
           element_at(array_sort(array_agg(CASE WHEN event_name IS NOT NULL THEN struct(ocurred_on, event_name) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).event_name AS event_name,
           element_at(array_sort(array_agg(CASE WHEN event_id IS NOT NULL THEN struct(ocurred_on, event_id) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).event_id AS event_id,
           max(ocurred_on) AS ocurred_on
    FROM f_origination_termination_events_br_logs
    GROUP BY 1,2
)
,
last_termination_all_event_br AS (
    -- USING ALL EVENTS (GLOBAL+INNER STAGE) TERMINATION EVENTS (LAST NON-NULL VALUE TECHNIQUE TO AVOID WINDOW_FUNCTIONS)
    SELECT application_id,
           'all' AS debug_termination_source,
           element_at(array_sort(array_agg(CASE WHEN event_type IS NOT NULL THEN struct(ocurred_on, event_type) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).event_type AS event_type,
           element_at(array_sort(array_agg(CASE WHEN journey_stage_name IS NOT NULL THEN struct(ocurred_on, journey_stage_name) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).journey_stage_name AS journey_stage_name,
           element_at(array_sort(array_agg(CASE WHEN event_name IS NOT NULL THEN struct(ocurred_on, event_name) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).event_name AS event_name,
           element_at(array_sort(array_agg(CASE WHEN event_id IS NOT NULL THEN struct(ocurred_on, event_id) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).event_id AS event_id,
           max(ocurred_on) AS ocurred_on
    FROM f_origination_events_br_logs
    WHERE event_type IN ('ABANDONMENT','APPROVAL', 'DECLINATION','REJECTION')
    GROUP BY 1,2
)
,
application_stats_and_debug_start_br AS (
    -- TO GET # EVENTS AND DATA ABOUT FIRST EVENT (FIRST NON-NULL VALUE TECHNIQUE TO AVOID WINDOW_FUNCTIONS)
   SELECT application_id,
           count(1) AS num_events,
           min(ocurred_on) AS first_ocurred_on,
           element_at(array_sort(array_agg(CASE WHEN event_name IS NOT NULL THEN struct(ocurred_on, event_name) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).event_name AS first_event_name,
           element_at(array_sort(array_agg(CASE WHEN event_id IS NOT NULL THEN struct(ocurred_on, event_id) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.ocurred_on < RIGHT.ocurred_on THEN -1 WHEN LEFT.ocurred_on > RIGHT.ocurred_on THEN 1 WHEN LEFT.ocurred_on == RIGHT.ocurred_on THEN 0 END), 1).event_id AS first_event_id
   FROM f_origination_events_br_logs AS oe
   GROUP BY 1
)
,
application_last_event_termination_priority_br AS (
    -- MERGING RESULTS IN ORDER OF PRIORITY (GLOBAL TERMINATION> INNER STAGE TERMINATION > ANY)
   SELECT oe.application_id,
           oe.client_id,
           oe.journey_name,
           COALESCE(lte_g.event_type,lte_a.event_type, oe.event_type) AS last_event_type,
           COALESCE(lte_g.journey_stage_name,lte_a.journey_stage_name, oe.journey_stage_name) AS last_journey_stage_name,
           COALESCE(lte_g.event_name,lte_a.event_name, oe.event_name) AS last_event_name,
           COALESCE(lte_g.event_id,lte_a.event_id, oe.event_id) AS last_event_id,
           COALESCE(lte_g.ocurred_on,lte_a.ocurred_on, oe.last_event_ocurred_on_processed) AS last_ocurred_on,
           from_utc_timestamp(COALESCE(lte_g.ocurred_on,lte_a.ocurred_on, oe.last_event_ocurred_on_processed), 'America/Sao_Paulo') AS last_ocurred_on_brt,
           -- DEBUG METRICS FOR EFFICIENT ANALYSIS IN BATCH
           COALESCE(lte_g.debug_termination_source,lte_a.debug_termination_source, 'no_termination_yet') AS debug_termination_source,
           sd.num_events AS debug_num_events,
           CASE
               WHEN sd.first_event_name != 'ApplicationCreated' THEN 'no_application_created'
               WHEN sd.num_events = 1 THEN 'only_application_created'
               ELSE 'standard'
           END AS debug_first_event_category,
           NAMED_STRUCT('first_ocurred_on',sd.first_ocurred_on, 'first_event_name',sd.first_event_name, 'first_event_id', sd.first_event_id) AS debug_first_event
   FROM      f_origination_events_br              AS oe
   LEFT JOIN last_termination_global_event_br     AS lte_g ON oe.application_id = lte_g.application_id
   LEFT JOIN last_termination_all_event_br        AS lte_a ON oe.application_id = lte_a.application_id
   LEFT JOIN application_stats_and_debug_start_br AS sd    ON oe.application_id = sd.application_id
)

SELECT
    application_id,
    client_id,
    journey_name,
    last_event_type,
    last_journey_stage_name,
    last_event_name,
    last_event_id,
    last_ocurred_on,
    last_ocurred_on_brt,
    debug_termination_source,
    debug_num_events,
    debug_first_event_category,
    debug_first_event,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM application_last_event_termination_priority_br
