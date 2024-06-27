
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


--raw_modeling.pseinvoiceurlnotobtainedco_co
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
    json_tmp.application.fullPayment.pse.amount AS payment_amount,
    TO_TIMESTAMP(json_tmp.application.fullPayment.pse.createdAt) AS payment_created_at,
    json_tmp.application.fullPayment.pse.financialInstitution.code AS payment_financial_institution_code,
    json_tmp.application.fullPayment.pse.financialInstitution.name AS payment_financial_institution_name,
    NULLIF(TRIM(json_tmp.application.fullPayment.pse.payer.email.address),'') AS payer_email,
    NULLIF(TRIM(json_tmp.application.fullPayment.pse.payer.nationalIdentification.number),'') AS payer_id_number,
    json_tmp.application.fullPayment.pse.payer.nationalIdentification.type AS payer_id_type,
    json_tmp.application.fullPayment.pse.payer.type AS payer_type,
    json_tmp.application.fullPayment.pse.provider AS payment_provider,
    --json_tmp.application.fullPayment.pse.redirectUrl AS payment_redirect_url,
    json_tmp.application.fullPayment.pse.reference AS payment_reference,
    json_tmp.application.fullPayment.pse.status AS payment_status,
    json_tmp.application.fullPayment.pse.statusReason AS payment_status_reason,
    json_tmp.application.fullPayment.pse.transactionId AS payment_transaction_id,
    COALESCE(CAST(json_tmp.application.fullPayment.pse.isMarketingOptIn AS BOOLEAN),
             CAST(json_tmp.client.isMarketingOptIn AS BOOLEAN),
             CAST(json_tmp.application.fullPayment.pse.payer.isMarketingOptIn AS BOOLEAN)) AS is_marketing_opt_in
    -- CUSTOM ATTRIBUTES
      -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS pseinvoiceurlnotobtainedco_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'pseinvoiceurlnotobtainedco_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
