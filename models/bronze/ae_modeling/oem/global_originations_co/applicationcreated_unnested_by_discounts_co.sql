{{
    config(
        materialized='incremental',
        unique_key='custom_event_discount_idx_pairing_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH explode_items as (
    SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.ally.slug AS ally_slug,
    json_tmp.application.channel AS channel,
    json_tmp.order.id AS order_id,
    json_tmp.application.product AS product,
    json_tmp.ally.store.slug AS store_slug,
    json_tmp.application.discountV2.addiPercentageAssumed AS total_discount_percentage_assumed_by_addi,
    json_tmp.application.discountV2.allyPercentageAssumed AS total_discount_percentage_assumed_by_ally,
    json_tmp.application.discountV2.percentage AS total_discount_percentage,
    explode(json_tmp.application.discountV2.details) AS discount_detail,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    'ORIGINATIONS_V2' AS custom_platform_version
    -- CAST(ocurred_on AS TIMESTAMP) AS applicationcreated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'applicationcreated_co') }}

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
)


SELECT 
    MD5(CONCAT(event_id,'_DID_',row_number() OVER (partition by event_id ORDER BY 'a'))) AS custom_event_discount_idx_pairing_id,
    MD5(CONCAT(application_id,'_DID_',row_number() OVER (partition by application_id ORDER BY 'a'))) AS custom_application_discount_idx_pairing_id,
    event_name_original,
    event_name,
    event_id,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    application_id,
    client_id,
    ally_slug,
    store_slug,
    channel,
    product,
    custom_platform_version,
    order_id,
    total_discount_percentage_assumed_by_addi,
    total_discount_percentage_assumed_by_ally,
    total_discount_percentage,
    -- EXPLODED ATTRIBUTES
    discount_detail.addiPercentageAssumed AS discount_percentage_assumed_by_addi,
    discount_detail.allyPercentageAssumed AS discount_percentage_assumed_by_ally,
    discount_detail.amount AS discount_amount,
    discount_detail.percentage AS discount_percentage,
    discount_detail.subOrderId AS discount_sub_order_id,
    discount_detail.type AS discount_type
FROM explode_items
