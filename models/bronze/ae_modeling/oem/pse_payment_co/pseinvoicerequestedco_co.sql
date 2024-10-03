
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


--raw_modeling.pseinvoicerequestedco_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    json_tmp.application.id AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.client.type AS client_type,
    json_tmp.originationEventType AS event_type,
    json_tmp.metadata.context.allyId AS ally_slug,
    json_tmp.application.journey.name AS journey_name,
    json_tmp.application.journey.currentStage.name AS journey_stage_name,
    json_tmp.application.channel AS channel,
    json_tmp.application.product AS product,
    -- PSE stage mapping
    COALESCE(json_tmp.payment.amount, json_tmp.application.fullPayment.pse.amount) AS payment_amount,
    --COALESCE(json_tmp.payment.asyncUrl, json_tmp.application.fullPayment.pse.asyncUrl) AS payment_async_url,
    TO_TIMESTAMP(COALESCE(json_tmp.payment.createdAt, json_tmp.application.fullPayment.pse.createdAt)) AS payment_created_at,
    --TO_TIMESTAMP(json_tmp.application.fullPayment.pse.finalizedAt) AS payment_finalized_at,
    COALESCE(json_tmp.payment.financialInstitution.code, json_tmp.application.fullPayment.pse.financialInstitution.code) AS payment_financial_institution_code,
    COALESCE(json_tmp.payment.financialInstitution.name, json_tmp.application.fullPayment.pse.financialInstitution.name) AS payment_financial_institution_name,
    COALESCE(json_tmp.payment.payer.cellPhone.countryCallingCode, json_tmp.application.fullPayment.pse.payer.cellPhone.countryCallingCode) AS payer_cellphone_country_code,
    NULLIF(TRIM(COALESCE(json_tmp.payment.payer.cellPhone.number, json_tmp.application.fullPayment.pse.payer.cellPhone.number)),'') AS payer_cellphone_number,
    NULLIF(TRIM(COALESCE(json_tmp.payment.payer.email.address, json_tmp.application.fullPayment.pse.payer.email.address)),'') AS payer_email,
    NULLIF(TRIM(COALESCE(json_tmp.payment.payer.fullName, json_tmp.application.fullPayment.pse.payer.fullName)),'') AS payer_full_name,
    NULLIF(TRIM(COALESCE(json_tmp.payment.payer.nationalIdentification.number, json_tmp.application.fullPayment.pse.payer.nationalIdentification.number)),'') AS payer_id_number,
    COALESCE(json_tmp.payment.payer.nationalIdentification.type, json_tmp.application.fullPayment.pse.payer.nationalIdentification.type) AS payer_id_type,
    COALESCE(json_tmp.payment.payer.type, json_tmp.application.fullPayment.pse.payer.type) AS payer_type,
    COALESCE(json_tmp.payment.provider, json_tmp.application.fullPayment.pse.provider) AS payment_provider,
    --COALESCE(json_tmp.payment.redirectUrl, json_tmp.application.fullPayment.pse.redirectUrl) AS payment_redirect_url,
    COALESCE(json_tmp.payment.reference, json_tmp.application.fullPayment.pse.reference) AS payment_reference,
    COALESCE(json_tmp.payment.status, json_tmp.application.fullPayment.pse.status) AS payment_status,
    COALESCE(CAST(json_tmp.application.fullPayment.pse.isMarketingOptIn AS BOOLEAN),
             CAST(json_tmp.payment.isMarketingOptIn AS BOOLEAN),
             CAST(json_tmp.client.isMarketingOptIn AS BOOLEAN),
             CAST(json_tmp.application.fullPayment.pse.payer.isMarketingOptIn AS BOOLEAN),
             CAST(json_tmp.payment.payer.isMarketingOptIn AS BOOLEAN)) AS is_marketing_opt_in
    --COALESCE(json_tmp.payment.statusReason, json_tmp.application.fullPayment.pse.statusReason) AS payment_status_reason,
    --COALESCE(json_tmp.payment.transactionId, json_tmp.application.fullPayment.pse.transactionId) AS payment_transaction_id,
    --COALESCE(json_tmp.payment.trazabilityCode, json_tmp.application.fullPayment.pse.trazabilityCode) AS payment_trazability_code
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS pseinvoicerequestedco_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'pseinvoicerequestedco_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
