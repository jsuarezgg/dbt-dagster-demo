


--raw_modeling.applicationcreated_br
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.ally.slug AS ally_slug,
    CAST(NULL AS STRING) AS journey_name,
    CAST(NULL AS STRING) AS journey_stage_name,
    json_tmp.application.channel AS channel,
    json_tmp.application.product AS product,
    json_tmp.client.cellPhone.number AS application_cellphone,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS application_date,
    json_tmp.client.email.address AS application_email,
    json_tmp.client.checkoutType AS client_checkout_type,
    json_tmp.client.nationalIdentification.number AS id_number,
    json_tmp.client.nationalIdentification.type AS id_type,
    json_tmp.order.id AS order_id,
    json_tmp.order.type AS order_type,
    json_tmp.application.requestedAmount AS requested_amount,
    json_tmp.application.requestedAmountWithoutDiscount AS requested_amount_without_discount,
    json_tmp.order.shippingAddress.lineOne AS shipping_address,
    json_tmp.order.shippingAddress.city AS shipping_city,
    json_tmp.ally.store.slug AS store_slug,
    json_tmp.user.id AS store_user_id,
    -- CUSTOM ATTRIBUTES
    'ORIGINATIONS_V2' as custom_platform_version
    -- CAST(ocurred_on AS TIMESTAMP) AS applicationcreated_br_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from raw_modeling.applicationcreated_br
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
