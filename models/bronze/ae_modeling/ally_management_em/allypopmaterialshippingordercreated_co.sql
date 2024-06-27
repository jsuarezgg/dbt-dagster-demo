
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


--raw_modeling.allypopmaterialshippingordercreated_co
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
    json_tmp.shippingOrder.shippingMethod AS shipping_method,
    json_tmp.shippingOrder.ally.slug AS ally_slug,
    COALESCE(json_tmp.shippingOrder.store.slug,json_tmp.metadata.context.storeId) AS store_slug,
    TO_TIMESTAMP(json_tmp.shippingOrder.createdAt) AS shipping_order_created_at,
    json_tmp.shippingOrder.buyer.email AS buyer_email,
    json_tmp.shippingOrder.buyer.fullName AS buyer_full_name,
    json_tmp.shippingOrder.buyer.phoneNumber AS buyer_phone_number,
    json_tmp.shippingOrder.deliveryInfo.address AS delivery_address,
    NULLIF(TRIM(json_tmp.shippingOrder.deliveryInfo.addressReference),'') AS delivery_address_reference,
    json_tmp.shippingOrder.deliveryInfo.city.code AS delivery_city_code,
    json_tmp.shippingOrder.deliveryInfo.city.name AS delivery_city_name,
    json_tmp.shippingOrder.deliveryInfo.deliveryRecipient.recipientFullName AS delivery_recipient_full_name,
    json_tmp.shippingOrder.deliveryInfo.deliveryRecipient.recipientPhoneNumber AS delivery_recipient_phone_number,
    COALESCE(json_tmp.shippingOrder.deliveryInfo.region.code, json_tmp.shippingOrder.deliveryInfo.city.region.code) AS delivery_region_code,
    COALESCE(json_tmp.shippingOrder.deliveryInfo.region.country.code, json_tmp.shippingOrder.deliveryInfo.city.region.country.code) AS delivery_country_code,
    COALESCE(json_tmp.shippingOrder.deliveryInfo.region.country.name, json_tmp.shippingOrder.deliveryInfo.city.region.country.name) AS delivery_country_name,
    COALESCE(json_tmp.shippingOrder.deliveryInfo.region.name, json_tmp.shippingOrder.deliveryInfo.city.region.name) AS delivery_region_name
    --(json_tmp.shippingOrder.store.isActive::BOOLEAN) AS shipping_order_store_is_active,
    --json_tmp.shippingOrder.store.name AS shipping_order_store_name,
    -- CAST(ocurred_on AS TIMESTAMP) AS allypopmaterialshippingordercreated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'allypopmaterialshippingordercreated_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}