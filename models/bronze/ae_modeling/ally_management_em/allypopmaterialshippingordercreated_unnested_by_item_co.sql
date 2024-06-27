
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

--raw_modeling.allypopmaterialshippingordercreated_co
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
    json_tmp.shippingOrder.id AS shipping_order_id,
    json_tmp.shippingOrder.number AS shipping_order_number,
    json_tmp.shippingOrder.ally.slug AS ally_slug,
    COALESCE(json_tmp.shippingOrder.store.slug,json_tmp.metadata.context.storeId) AS store_slug,
    TO_TIMESTAMP(json_tmp.shippingOrder.createdAt) AS shipping_order_created_at,
    explode(json_tmp.shippingOrder.items) AS shipping_order_item

    -- CUSTOM ATTRIBUTES
     -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS allypopmaterialshippingordercreated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'allypopmaterialshippingordercreated_co') }}

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
)


SELECT 
    CONCAT('EID_',event_id,
           CONCAT('__SOID_',shipping_order_id,'_RN_',ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY COALESCE(shipping_order_item.sku,'NONE'),COALESCE(shipping_order_item.quantity::STRING,'NONE')))
           ) AS surrogate_key,
    CONCAT('SOID_',shipping_order_id,'_RN_',ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY COALESCE(shipping_order_item.sku,'NONE'),COALESCE(shipping_order_item.quantity::STRING,'NONE'))) AS custom_item_id,
	event_name_original,
	event_name,
	event_id,
	ocurred_on,
	ocurred_on_date,
	ingested_at,
	updated_at,
	shipping_order_id,
    shipping_order_number,
    ally_slug,
    store_slug,
    shipping_order_created_at,
    shipping_order_item.quantity AS item_quantity,
    shipping_order_item.sku AS item_sku
FROM explode_items