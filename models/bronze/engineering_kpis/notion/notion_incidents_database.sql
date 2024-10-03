{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.notion_page
SELECT 
    _airbyte_data.archived AS archived,
    _airbyte_data.created_time AS created_time,
    _airbyte_data.id AS page_id,
    _airbyte_data.properties[4].value.checkbox AS autodetected,
    _airbyte_data.properties[9].value.date.start AS incident_date,
    _airbyte_data.properties[16].value.title.text.content AS incident_name,
    _airbyte_data.properties[2].value.multi_select.name AS incident_owner,
    _airbyte_data.properties[0].value.select.name AS severity,
    _airbyte_data.properties[10].value.select.name AS root_cause,
    _airbyte_data.properties[6].value.checkbox AS socialized_postmortem,
    _airbyte_data.properties[5].value.select.name AS status,
    _airbyte_data.properties[3].value.number AS ttd,
    _airbyte_data.properties[12].value.number AS ttr,
    _airbyte_data.properties[15].value.multi_select.name AS affected_tiers,
    _airbyte_data.properties[14].value.number AS downtime_duration,
    _airbyte_data.url AS incident_url
FROM {{ source('raw', 'notion_page') }}
where _airbyte_data.parent.database_id = '3601eaf2-9e32-459e-85db-55aaae5568a2'
QUALIFY row_number() OVER (PARTITION BY _airbyte_data.id ORDER BY _airbyte_data.last_edited_time DESC, _airbyte_emitted_at DESC) = 1