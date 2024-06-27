{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    event.event_name_original,
    event.event_name,
    event.event_id,
    event.ocurred_on,
    event.ocurred_on_date,
    event.ingested_at,
    event.updated_at,
    event.changed_by,
    event.reason,
    uaa.additional_attributes_key,
    uaa.additional_attributes_value,
    uapp.application_id,
    ucli.client_id
FROM {{ ref('frauddecisionmade_co') }} event
LEFT JOIN {{ ref('frauddecisionmade_co_unnested_by_additional_attribute') }} uaa    ON event.event_id = uaa.event_id
LEFT JOIN {{ ref('frauddecisionmade_co_unnested_by_application') }} uapp            ON event.event_id = uapp.event_id
LEFT JOIN {{ ref('frauddecisionmade_co_unnested_by_client') }} ucli                 ON event.event_id = ucli.event_id