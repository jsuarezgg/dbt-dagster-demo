
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


--raw_modeling.allycreated_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    COALESCE(json_tmp.ally.slug, json_tmp.metadata.context.allyId) AS ally_slug,
    json_tmp.ally.channel AS ally_channel,
    json_tmp.ally.name AS ally_name,
    json_tmp.ally.state AS ally_state,
    json_tmp.ally.webpage AS ally_webpage,
    NULLIF(TRIM(json_tmp.ally.additionalInformation.address.additionalInformation),'') AS ally_address_additional_information,
    NULLIF(TRIM(json_tmp.ally.additionalInformation.address.city),'') AS ally_address_city,
    json_tmp.ally.additionalInformation.address.lineOne AS ally_address_line_one,
    --json_tmp.ally.additionalInformation.address.postalCode AS ally_address_postal_code,
    NULLIF(TRIM(json_tmp.ally.additionalInformation.address.state),'') AS ally_address_state,
    --json_tmp.ally.additionalInformation.financialInformation.agency AS ally_financial_information_agency,
    json_tmp.ally.additionalInformation.financialInformation.bankAccount AS ally_financial_information_bank_account,
    json_tmp.ally.additionalInformation.financialInformation.bankAccountType AS ally_financial_information_bank_account_type,
    json_tmp.ally.additionalInformation.financialInformation.bankCode AS ally_financial_information_bank_code,
    json_tmp.ally.additionalInformation.financialInformation.bankName AS ally_financial_information_bank_name,
    --json_tmp.ally.additionalInformation.financialInformation.ispbCode AS ally_financial_information_ispb_code,
    json_tmp.ally.additionalInformation.financialInformation.name AS ally_financial_information_name,
    json_tmp.ally.additionalInformation.financialInformation.number AS ally_financial_information_number,
    NULLIF(TRIM(json_tmp.ally.additionalInformation.financialInformation.numberCheckDigit),'') AS ally_financial_information_number_check_digit,
    json_tmp.ally.additionalInformation.financialInformation.type AS ally_financial_information_type,
    NULLIF(TRIM(json_tmp.ally.additionalInformation.legalIdentification.checkDigit),'') AS ally_legal_identification_check_digit,
    json_tmp.ally.additionalInformation.legalIdentification.name AS ally_legal_identification_name,
    json_tmp.ally.additionalInformation.legalIdentification.number AS ally_legal_identification_number,
    json_tmp.ally.additionalInformation.legalIdentification.taxPayerType AS ally_legal_identification_tax_payer_type,
    json_tmp.ally.additionalInformation.legalIdentification.type AS ally_legal_identification_type,
    json_tmp.ally.additionalInformation.notificationInformation.email AS ally_notification_information_email,
    --json_tmp.ally.additionalInformation.notificationInformation.name AS ally_notification_information_name,
    json_tmp.ally.additionalInformation.notificationInformation.phone AS ally_notification_information_phone
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS allycreated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'allycreated_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
