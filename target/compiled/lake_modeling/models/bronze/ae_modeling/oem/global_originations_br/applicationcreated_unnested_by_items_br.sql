

WITH explode_items as (SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.id AS application_id,
    json_tmp.ocurredOn AS date,
    json_tmp.application.wasRestarted AS checkpoint_application,
    json_tmp.client.id AS client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.ally.slug AS ally_slug,
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
    json_tmp.ally.store.slug AS store_slug,
    json_tmp.user.id AS store_user_id,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    CAST(1 AS TINYINT) AS application_created_step,
    'ORIGINATIONS_V2' AS custom_platform_version,
    CAST(ocurred_on AS TIMESTAMP) AS application_date,
    explode(json_tmp.order.items) exp_items
    -- CAST(ocurred_on AS TIMESTAMP) AS applicationcreated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from raw_modeling.applicationcreated_br


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")

)

SELECT 
CONCAT('EID_',event_id,'_IPI_',row_number() OVER (partition by event_id order by ocurred_on)) AS surrogate_key,
event_name_original,
event_name,
event_id,
ocurred_on,
ocurred_on_date,
ingested_at,
-- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
application_id,
date,
checkpoint_application,
client_id,
client_type,
event_type,
ally_slug,
application_cellphone,
application_email,
id_number,
id_type,
journey_name,
journey_stage_name,
channel,
order_id,
product,
requested_amount,
requested_amount_without_discount,
store_slug,
store_user_id,
-- CUSTOM ATTRIBUTES
-- Fill with your custom attributes
application_created_step,
custom_platform_version,
application_date,
exp_items.brand item_brand,
exp_items.category item_category,
exp_items.discount item_discount,
exp_items.name item_name,
exp_items.pictureurl item_pictureurl,
exp_items.quantity item_quantity,
exp_items.sku item_sku,
exp_items.taxamount item_taxamount,
exp_items.unitprice item_unitprice
FROM explode_items
-- DBT INCREMENTAL SENTENCE