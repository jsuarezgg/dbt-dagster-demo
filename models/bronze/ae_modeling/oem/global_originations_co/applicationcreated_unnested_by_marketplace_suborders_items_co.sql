
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='custom_event_suborder_sku_pairing_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw_modeling.applicationcreated_co
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
    json_tmp.order.id AS order_id,
    json_tmp.application.product AS product,
    -- json_tmp.application.channel AS channel,--debug
    explode(json_tmp.order.subOrders) AS suborder,
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
,
sub_explode_items AS (
    SELECT
        event_name_original,
        event_name,
        event_id,
        ocurred_on,
        ocurred_on_date,
        ingested_at,
        updated_at,
        application_id,
        client_id,
        ally_slug,
        order_id,
        product,
        suborder.id AS suborder_id,
        suborder.ally.slug AS suborder_ally_slug,
        explode(suborder.items) AS suborder_item
        --suborder.totalAmount AS suborder_total_amount,
        --suborder.totalWithoutDiscountAmount AS suborder_total_amount_without_discount,
        --suborder.ally.name AS suborder_ally_name,
        --suborder.ally.store.slug AS suborder_store_slug,
        --suborder.ally.configuration.marketplaceFees.purchaseFee AS suborder_marketplace_purchase_fee
        --suborder.ally.configuration.productPolicies -- list
        --suborder.ally.configuration.gatewayPartners -- list
    FROM explode_items
)

SELECT
        -- Level 1 Nesting custom keys
        MD5(CONCAT(event_id, suborder_id)) AS custom_event_suborder_pairing_id,
        MD5(CONCAT(application_id, suborder_id)) AS custom_application_suborder_pairing_id,
        -- Level 2 Nesting custom keys
        MD5(CONCAT(
            application_id,
            suborder_id,
            COALESCE(suborder_item.sku,CONCAT('_SKU_NOT_FOUND_',ROW_NUMBER() OVER (PARTITION BY event_id,suborder_id ORDER BY 'NO_SKU')))
        )) AS custom_application_suborder_sku_pairing_id,
        MD5(CONCAT(
            event_id,
            suborder_id,
            COALESCE(suborder_item.sku,CONCAT('_SKU_NOT_FOUND_',ROW_NUMBER() OVER (PARTITION BY event_id,suborder_id ORDER BY 'NO_SKU')))
        )) AS custom_event_suborder_sku_pairing_id,
        -- Fields
        event_name_original,
        event_name,
        event_id,
        ocurred_on,
        ocurred_on_date,
        ingested_at,
        updated_at,
        application_id,
        client_id,
        ally_slug,
        order_id,
        product,
        suborder_id,
        suborder_ally_slug,
        ROW_NUMBER() OVER (PARTITION BY event_id,suborder_id ORDER BY 'NO_SKU') AS custom_suborder_item_idx,
        COALESCE(
            suborder_item.sku,
            CONCAT('_SKU_NOT_FOUND_',
                ROW_NUMBER() OVER (PARTITION BY event_id,suborder_id ORDER BY 'NO_SKU')
            )
        ) AS suborder_item_sku,
        suborder_item.name AS suborder_item_name,
        suborder_item.unitPrice AS suborder_item_unit_price,
        suborder_item.quantity AS suborder_item_quantity,
        suborder_item.discount AS suborder_item_discount,
        suborder_item.sellerId AS suborder_item_seller_id
FROM sub_explode_items