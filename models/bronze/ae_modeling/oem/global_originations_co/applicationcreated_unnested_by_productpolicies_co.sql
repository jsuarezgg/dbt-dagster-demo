{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='surrogate_key',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH select_explode as (
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
    EXPLODE(json_tmp.ally.configuration.productPolicies) as ally_product_policies,
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
    CONCAT('EID_',event_id,'_APPID_',application_id,'_TYPE_',ally_product_policies.type) AS surrogate_key,
    CONCAT('_APPID_',application_id,'_TYPE_',ally_product_policies.type) AS application_product_policy_id,
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
    --- ALLY PRODUCT POLICIES FIELDS
    ally_product_policies.mdfFees.cancellationMDF as cancellation_mdf,
    ally_product_policies.mdfFees.fraudMDF as fraud_mdf,
    ally_product_policies.mdfFees.originationMDF as origination_mdf,
    ally_product_policies.type as type,
    ally_product_policies.maxAmount as max_amount
FROM select_explode