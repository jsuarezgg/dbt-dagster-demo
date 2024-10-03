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

-- raw_backend_events.kyc_event_seonfraudobtained_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp(NOW()) AS updated_at,
    -- MAPPED FIELDS
    json_tmp.applicationId AS application_id,
    json_tmp.clientId AS client_id,
    json_tmp.seonFraudResponse.data.state AS seon_state,
    json_tmp.seonFraudResponse.data.fraudScore AS seon_fraud_score,
    json_tmp.seonFraudResponse.data.appliedRules AS seon_applied_rules,
    json_tmp.seonFraudResponse.data.ipDetails.score AS seon_ip_details_score,
    json_tmp.seonFraudResponse.data.emailDetails.score AS seon_email_details_score,
    json_tmp.seonFraudResponse.data.emailDetails.domainDetails.created::TIMESTAMP AS seon_email_details_domain_created,
    json_tmp.seonFraudResponse.data.emailDetails.domainDetails.updated::TIMESTAMP AS seon_email_details_domain_updated,
    json_tmp.seonFraudResponse.data.emailDetails.accountDetails AS seon_email_details_account_details,
    json_tmp.seonFraudResponse.data.emailDetails.breachDetails.haveibeenpwnedListed AS seon_email_details_breach_have_i_been_pwned_listed,
    json_tmp.seonFraudResponse.data.emailDetails.breachDetails.numberOfBreaches AS seon_email_details_breach_number_of_breaches,
    json_tmp.seonFraudResponse.data.emailDetails.breachDetails.firstBreach::DATE AS seon_email_details_breach_first_breach,
    json_tmp.seonFraudResponse.data.phoneDetails.score AS seon_phone_details_score,
    json_tmp.seonFraudResponse.data.phoneDetails.carrier AS seon_phone_details_carrier,
    json_tmp.seonFraudResponse.data.phoneDetails.accountDetails AS seon_phone_details_account_details,
    json_tmp.seonFraudResponse.data.amlDetails.hasWatchlistMatch AS seon_aml_details_has_watchlist_match,
    json_tmp.seonFraudResponse.data.amlDetails.hasSanctionMatch AS seon_aml_details_has_sanction_match,
    json_tmp.seonFraudResponse.data.amlDetails.hasCrimelistMatch AS seon_aml_details_has_crimelist_match,
    json_tmp.seonFraudResponse.data.amlDetails.hasPepMatch AS seon_aml_details_has_pep_match
FROM {{ source('raw_backend_events', 'kyc_event_seonfraudobtained_co') }}
{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}" - INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}" - INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
