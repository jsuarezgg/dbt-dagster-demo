
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


--raw_modeling.applicationcreated_co
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
    COALESCE(json_tmp.application.id, CONCAT('UNKNOWN_FROM_EVENT_ID_', json_tmp.eventId)) AS application_id,
    json_tmp.ocurredOn AS date,
    json_tmp.application.wasRestarted AS checkpoint_application,
    json_tmp.client.id AS client_id,
    json_tmp.client.isTransactionalBased AS client_is_transactional_based,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.ally.slug AS ally_slug,
    json_tmp.order.subOrders.ally.slug AS suborders_ally_slug_array,
    json_tmp.client.cellPhone.number AS application_cellphone,
    json_tmp.client.email.address AS application_email,
    json_tmp.client.nationalIdentification.number AS id_number,
    json_tmp.client.nationalIdentification.type AS id_type,
    CAST(NULL AS STRING) AS journey_name,
    CAST(NULL AS STRING) AS journey_stage_name,
    json_tmp.application.channel AS channel,
    json_tmp.order.id AS order_id,
    json_tmp.application.product AS product,
    json_tmp.application.requestedAmount AS requested_amount,
    json_tmp.application.requestedAmountWithoutDiscount AS requested_amount_without_discount,
    -- ORDER LEGACY (TO BE DELETED AFTER TARGETS REFACTOR THIS TO THE NEW COLUMN NAMES)
    json_tmp.order.shippingAddress.lineOne AS shipping_address,
    json_tmp.order.shippingAddress.city AS shipping_city,
    --- START OF ORDER REFACTOR
    json_tmp.order.currency AS order_currency,
    json_tmp.order.description AS order_description,
    json_tmp.order.shippingAmount AS order_shipping_amount,
    json_tmp.order.taxesTotalAmount AS order_taxes_total_amount,
    json_tmp.order.totalAmount AS order_total_amount,
    json_tmp.order.totalWithoutDiscountAmount AS order_total_without_discount_amount,
    json_tmp.order.type AS order_type,
    json_tmp.order.geolocation.latitude AS order_geolocation_latitude,
    json_tmp.order.geolocation.longitude AS order_geolocation_longitude,
    json_tmp.order.billingAddress.city AS order_billing_address_city,
    json_tmp.order.billingAddress.complement AS order_billing_address_complement,
    json_tmp.order.billingAddress.country AS order_billing_address_country,
    NULLIF(TRIM(json_tmp.order.billingAddress.lineOne),'') AS order_billing_address_line_one,
    json_tmp.order.billingAddress.neighborhood AS order_billing_address_neighborhood,
    json_tmp.order.billingAddress.number AS order_billing_address_number,
    json_tmp.order.billingAddress.postalCode AS order_billing_address_postal_code,
    json_tmp.order.billingAddress.street AS order_billing_address_street,
    json_tmp.order.billingAddress.subCountry AS order_billing_address_sub_country,
    json_tmp.order.pickUpAddress.city AS order_pick_up_address_city,
    json_tmp.order.pickUpAddress.country AS order_pick_up_address_country,
    NULLIF(TRIM(json_tmp.order.pickUpAddress.lineOne),'') AS order_pick_up_address_line_one,
    json_tmp.order.shippingAddress.city AS order_shipping_address_city,
    json_tmp.order.shippingAddress.complement AS order_shipping_address_complement,
    json_tmp.order.shippingAddress.country AS order_shipping_address_country,
    NULLIF(TRIM(json_tmp.order.shippingAddress.lineOne),'') AS order_shipping_address_line_one,
    json_tmp.order.shippingAddress.neighborhood AS order_shipping_address_neighborhood,
    json_tmp.order.shippingAddress.number AS order_shipping_address_number,
    json_tmp.order.shippingAddress.postalCode AS order_shipping_address_postal_code,
    json_tmp.order.shippingAddress.street AS order_shipping_address_street,
    json_tmp.order.shippingAddress.subCountry AS order_shipping_address_sub_country,
    CAST(json_tmp.order.shoppingIntent.attributable AS BOOLEAN) AS order_shopping_intent_attributable,
    json_tmp.order.shoppingIntent.id as order_shopping_intent_id,
    --- END OF ORDER REFACTOR
    json_tmp.ally.store.slug AS store_slug,
    json_tmp.user.id AS store_user_id,
    -- CUSTOM ATTRIBUTES
    CAST(1 AS TINYINT) AS application_created_step,
    'ORIGINATIONS_V2' AS custom_platform_version,
    CAST(ocurred_on AS TIMESTAMP) AS application_date
    -- CAST(ocurred_on AS TIMESTAMP) AS applicationcreated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'applicationcreated_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}