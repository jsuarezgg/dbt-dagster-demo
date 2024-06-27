{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--raw_modeling.telesignphoneidobtained_v2_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.metadata.context.traceId AS application_id,
    json_tmp.client.id AS client_id,
    json_tmp.telesignPhoneId.blocklisting.blockCode AS telesignPhoneId_blocklisting_blockCode,
    json_tmp.telesignPhoneId.blocklisting.blockDescription AS telesignPhoneId_blocklisting_blockDescription,
    json_tmp.telesignPhoneId.blocklisting.blocked AS telesignPhoneId_blocklisting_blocked,
    json_tmp.telesignPhoneId.carrier.name AS telesignPhoneId_carrier_name,
    json_tmp.telesignPhoneId.location.city AS telesignPhoneId_location_city,
    json_tmp.telesignPhoneId.location.country.iso2 AS telesignPhoneId_location_country_iso2,
    json_tmp.telesignPhoneId.location.country.iso3 AS telesignPhoneId_location_country_iso3,
    json_tmp.telesignPhoneId.location.country.name AS telesignPhoneId_location_country_name,
    json_tmp.telesignPhoneId.numbering.cleansing.call.cleansedCode AS telesignPhoneId_numbering_cleansing_call_cleansedCode,
    json_tmp.telesignPhoneId.numbering.cleansing.call.countryCode AS telesignPhoneId_numbering_cleansing_call_countryCode,
    json_tmp.telesignPhoneId.numbering.cleansing.call.maxLength AS telesignPhoneId_numbering_cleansing_call_maxLength,
    json_tmp.telesignPhoneId.numbering.cleansing.call.minLength AS telesignPhoneId_numbering_cleansing_call_minLength,
    json_tmp.telesignPhoneId.numbering.cleansing.call.phoneNumber AS telesignPhoneId_numbering_cleansing_call_phoneNumber,
    json_tmp.telesignPhoneId.numbering.cleansing.sms.cleansedCode AS telesignPhoneId_numbering_cleansing_sms_cleansedCode,
    json_tmp.telesignPhoneId.numbering.cleansing.sms.countryCode AS telesignPhoneId_numbering_cleansing_sms_countryCode,
    json_tmp.telesignPhoneId.numbering.cleansing.sms.maxLength AS telesignPhoneId_numbering_cleansing_sms_maxLength,
    json_tmp.telesignPhoneId.numbering.cleansing.sms.minLength AS telesignPhoneId_numbering_cleansing_sms_minLength,
    json_tmp.telesignPhoneId.numbering.cleansing.sms.phoneNumber AS telesignPhoneId_numbering_cleansing_sms_phoneNumber,
    json_tmp.telesignPhoneId.numbering.original.completePhoneNumber AS telesignPhoneId_numbering_original_completePhoneNumber,
    json_tmp.telesignPhoneId.numbering.original.countryCode AS telesignPhoneId_numbering_original_countryCode,
    json_tmp.telesignPhoneId.numbering.original.phoneNumber AS telesignPhoneId_numbering_original_phoneNumber,
    json_tmp.telesignPhoneId.phoneType.code AS telesignPhoneId_phoneType_code,
    json_tmp.telesignPhoneId.phoneType.description AS telesignPhoneId_phoneType_description,
    json_tmp.telesignPhoneId.portingHistory.numberOfPortings AS telesignPhoneId_portingHistory_numberOfPortings,
    TO_TIMESTAMP(json_tmp.telesignPhoneId.portingHistory.portingDateTime) AS telesignPhoneId_portingHistory_portingDateTime,
    json_tmp.telesignPhoneId.portingHistory.status.code AS telesignPhoneId_portingHistory_status_code,
    json_tmp.telesignPhoneId.portingHistory.status.description AS telesignPhoneId_portingHistory_status_description,
    json_tmp.telesignPhoneId.referenceId AS telesignPhoneId_referenceId,
    json_tmp.telesignPhoneId.simSwap.riskIndicator AS telesignPhoneId_simSwap_riskIndicator,
    json_tmp.telesignPhoneId.simSwap.status.code AS telesignPhoneId_simSwap_status_code,
    json_tmp.telesignPhoneId.simSwap.status.description AS telesignPhoneId_simSwap_status_description,
    TO_TIMESTAMP(json_tmp.telesignPhoneId.simSwap.swapDateTime) AS telesignPhoneId_simSwap_swapDateTime,
    json_tmp.telesignPhoneId.status.code AS telesignPhoneId_status_code,
    json_tmp.telesignPhoneId.status.description AS telesignPhoneId_status_description,
    TO_TIMESTAMP(json_tmp.telesignPhoneId.status.updatedOn) AS telesignPhoneId_status_updatedOn,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'V2' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS telesignphoneidobtained_v2_co_at -- To store it as a standalone column, when needed
FROM  {{source('raw_modeling', 'telesignphoneidobtained_v2_co' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
