
{{
    config(
        materialized=var('override_materialization', 'incremental'),
        unique_key='custom_event_suborder_product_policy_pairing_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw_modeling.bnpnpaymentapprovedco_co
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
    -- CAST(ocurred_on AS TIMESTAMP) AS bnpnpaymentapprovedco_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'bnpnpaymentapprovedco_co') }}

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
        explode(suborder.ally.configuration.productPolicies) AS suborder_product_policy
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
    MD5(CONCAT(application_id, suborder_id, suborder_product_policy.type)) AS custom_application_suborder_product_policy_pairing_id,
    MD5(CONCAT(event_id, suborder_id, suborder_product_policy.type)) AS custom_event_suborder_product_policy_pairing_id,
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
    suborder_product_policy.type AS product_policy_type, --https://github.com/AdelanteFinancialHoldings/platform/blob/master/backend-jvm-legacy/channels/kernel/domain/src/main/java/com/addi/channels/kernel/domain/application/AbstractApplication.java#L289
    suborder_product_policy.maxAmount AS product_policy_max_amount,
    suborder_product_policy.mdfFees.originationMDF AS product_policy_origination_mdf,
    suborder_product_policy.mdfFees.cancellationMDF AS product_policy_cancellation_mdf,
    suborder_product_policy.mdfFees.fraudMDF AS product_policy_fraud_mdf
FROM sub_explode_items