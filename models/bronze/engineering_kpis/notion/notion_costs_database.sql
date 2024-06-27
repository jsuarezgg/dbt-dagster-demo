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
SELECT DISTINCT
    _airbyte_data.id AS id,
    _airbyte_data.properties[0].value.number AS databricks,
    _airbyte_data.properties[1].value.number AS auth0,
    _airbyte_data.properties[2].value.number AS pagerduty,
    _airbyte_data.properties[3].value.number AS aws,
    _airbyte_data.properties[4].value.number AS astronomer,
    _airbyte_data.properties[5].value.title.plain_text AS month
FROM {{ source('raw', 'notion_page') }}
where _airbyte_data.parent.database_id = '7bcb3b72-572e-4ea7-9888-e59ff4026e4f'