
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
    explode(json_tmp.order.items) AS order_item,
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    'ORIGINATIONS_V2' AS custom_platform_version
    -- CAST(ocurred_on AS TIMESTAMP) AS applicationcreated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'applicationcreated_br') }}

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
)


SELECT
    CONCAT('EID_',event_id,'_OIN_',row_number() OVER (partition by event_id order by COALESCE(order_item.name,'NO_NAME'))) AS surrogate_key,
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
    -- ORDER LEGACY (TO BE DELETED AFTER TARGETS REFACTOR THIS TO THE NEW COLUMN NAMES)
    order_item.brand AS item_brand,
    order_item.category AS item_category,
    order_item.discount AS item_discount,
    order_item.name AS item_name,
    order_item.pictureurl AS item_pictureurl,
    order_item.quantity AS item_quantity,
    order_item.sku AS item_sku,
    order_item.taxamount AS item_taxamount,
    order_item.unitprice AS item_unitprice,
    --- START OF ORDER REFACTOR
    row_number() OVER (partition by event_id order by COALESCE(order_item.name,'NO_NAME')) AS order_item_row_number_by_item_name,
    NULLIF(TRIM(order_item.category),'') AS order_item_category,
    order_item.discount AS order_item_discount,
    order_item.name AS order_item_name,
    NULLIF(TRIM(order_item.pictureUrl),'') AS order_item_picture_url,
    order_item.quantity AS order_item_quantity,
    order_item.sku AS order_item_sku,
    order_item.taxAmount AS order_item_tax_amount,
    order_item.unitPrice AS order_item_unit_price
    --- END OF ORDER REFACTOR
FROM explode_items