
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='custom_event_suborder_pairing_id',
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


SELECT
    -- Level 1 Nesting custom keys
    MD5(CONCAT(event_id, suborder.id)) AS custom_event_suborder_pairing_id,
    MD5(CONCAT(application_id, suborder.id)) AS custom_application_suborder_pairing_id,
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
	suborder.id AS suborder_id,
	suborder.totalAmount AS suborder_total_amount,
	suborder.totalWithoutDiscountAmount AS suborder_total_amount_without_discount,
	suborder.shippingAmount AS suborder_shipping_amount,
	suborder.attributionWeights.byNumberOfAllies AS suborder_attribution_weight_by_number_of_allies,
	suborder.attributionWeights.byTotalAmount AS suborder_attribution_weight_by_total_amount,
	suborder.attributionWeights.byTotalWithoutDiscountAmount AS suborder_attribution_weight_by_total_without_discount_amount,
	suborder.externalIds AS suborder_vtex_external_id_array,
	suborder.ally.slug AS suborder_ally_slug,
    suborder.ally.marketplaceSellerIds AS suborder_vtex_seller_id_array,
	--suborder.ally.name AS suborder_ally_name,
	suborder.ally.store.slug AS suborder_store_slug,
	suborder.ally.configuration.marketplaceFees.purchaseFee AS suborder_marketplace_purchase_fee
	--suborder.ally.configuration.productPolicies -- list
    --suborder.ally.configuration.gatewayPartners -- list
FROM explode_items