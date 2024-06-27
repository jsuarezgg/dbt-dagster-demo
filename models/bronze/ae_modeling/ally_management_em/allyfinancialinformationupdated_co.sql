
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


--raw_modeling.allyfinancialinformationupdated_co
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
    json_tmp.metadata.context.allyId AS ally_slug,
    --json_tmp.financialInformation.agency AS ally_financial_information_agency,
    json_tmp.financialInformation.bankAccount AS ally_financial_information_bank_account,
    json_tmp.financialInformation.bankAccountType AS ally_financial_information_bank_account_type,
    json_tmp.financialInformation.bankCode AS ally_financial_information_bank_code,
    json_tmp.financialInformation.bankName AS ally_financial_information_bank_name,
    --json_tmp.financialInformation.ispbCode AS ally_financial_information_ispb_code,
    json_tmp.financialInformation.name AS ally_financial_information_name,
    json_tmp.financialInformation.number AS ally_financial_information_number,
    --json_tmp.financialInformation.numberCheckDigit AS ally_financial_information_number_check_digit,
    json_tmp.financialInformation.type AS ally_financial_information_type
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
    -- CAST(ocurred_on AS TIMESTAMP) AS allyfinancialinformationupdated_co_at -- To store it as a standalone column, when needed
-- DBT SOURCE REFERENCE
from {{ source('raw_modeling', 'allyfinancialinformationupdated_co') }}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_hours')}}" HOUR)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}
