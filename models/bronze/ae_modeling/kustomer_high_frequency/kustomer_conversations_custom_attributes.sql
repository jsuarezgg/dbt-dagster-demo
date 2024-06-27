{{
    config(
        materialized='incremental',
        unique_key='pairing_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--Volume: 303187965 (all               ) by 2023-09-19
--Volume:  32884630 (conversations-only) by 2023-09-19
--raw_modeling.kus_custom_attributes

WITH
kus_conversation_custom_attributes AS (
    SELECT
        --resourceName AS resource_name, -- filtered for conversations only
        MD5(CONCAT(resourceId,TRIM(customAttributeName))) AS pairing_id,
        resourceId AS conversation_id, -- filtered for conversations only
        TRIM(customAttributeName) AS custom_attribute_name,
        NULLIF(TRIM(customAttributeValue),'') AS custom_attribute_value,
        referenceDate AS reference_date,
        `year` AS reference_date_year,
        `month` AS reference_date_month
    FROM {{ source('raw_modeling', 'kus_custom_attributes') }}
    WHERE resourceName = 'conversations' AND NULLIF(TRIM(customAttributeName),'') IS NOT NULL AND resourceId IS NOT NULL
    {%- if is_incremental() %}
    -- DBT INCREMENTAL SENTENCE
    AND to_date(referenceDate) BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(referenceDate AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
    {%- endif -%}
)
,
{%- if is_incremental() %}
existing_records AS (
    SELECT
        pairing_id,
        conversation_id,
        custom_attribute_name,
        custom_attribute_value,
        reference_date,
        reference_date_year,
        reference_date_month
    FROM {{ this }}
    WHERE pairing_id IN (SELECT DISTINCT pairing_id FROM kus_conversation_custom_attributes)
)
,
{%- endif -%}
union_to_process AS (
    SELECT pairing_id, conversation_id, custom_attribute_name, custom_attribute_value, reference_date, reference_date_year, reference_date_month FROM kus_conversation_custom_attributes
    {% if is_incremental() -%}
    UNION ALL
    SELECT pairing_id, conversation_id, custom_attribute_name, custom_attribute_value, reference_date, reference_date_year, reference_date_month FROM existing_records
    {%- endif %}
)
,
last_value_per_pairing AS (
    SELECT
        pairing_id,
        conversation_id,
        custom_attribute_name,
        --LAST NON-NULL VALUE USING PROVED METHOD - on reference_date (as an homolog to the classic ocurredOn for this dataset)
        element_at(array_sort(array_agg(CASE WHEN custom_attribute_value IS NOT NULL THEN struct(reference_date, custom_attribute_value) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.reference_date < RIGHT.reference_date THEN 1 WHEN LEFT.reference_date > RIGHT.reference_date THEN -1 WHEN LEFT.reference_date == RIGHT.reference_date THEN 0 END), 1).custom_attribute_value AS custom_attribute_value,
        MAX(reference_date) AS reference_date,
        element_at(array_sort(array_agg(CASE WHEN reference_date_year IS NOT NULL THEN struct(reference_date, reference_date_year) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.reference_date < RIGHT.reference_date THEN 1 WHEN LEFT.reference_date > RIGHT.reference_date THEN -1 WHEN LEFT.reference_date == RIGHT.reference_date THEN 0 END), 1).reference_date_year AS reference_date_year,
        element_at(array_sort(array_agg(CASE WHEN reference_date_month IS NOT NULL THEN struct(reference_date, reference_date_month) ELSE NULL END), (LEFT, RIGHT) -> CASE WHEN LEFT.reference_date < RIGHT.reference_date THEN 1 WHEN LEFT.reference_date > RIGHT.reference_date THEN -1 WHEN LEFT.reference_date == RIGHT.reference_date THEN 0 END), 1).reference_date_month AS reference_date_month
    FROM union_to_process
    GROUP BY 1,2,3
)

SELECT
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    pairing_id,
    conversation_id,
    custom_attribute_name,
    custom_attribute_value,
    reference_date,
    reference_date_year,
    reference_date_month,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS data_platform_updated_at
-- DBT SOURCE REFERENCE
FROM last_value_per_pairing