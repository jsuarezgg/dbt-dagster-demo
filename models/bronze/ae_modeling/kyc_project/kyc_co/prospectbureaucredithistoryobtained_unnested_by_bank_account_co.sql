{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        incremental_strategy='merge',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
--raw_modeling.prospectbureaucredithistoryobtained_co
WITH select_explode AS (
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
        json_tmp.applicationId AS application_id,
        json_tmp.prospectId AS client_id,
        EXPLODE(json_tmp.commercial.bankAccounts) AS item
        -- CUSTOM ATTRIBUTES
        -- CAST(ocurred_on AS TIMESTAMP) AS prospectbureaucredithistoryobtained_co_at -- To store it as a standalone column, when needed
    FROM  {{source('raw_modeling', 'prospectbureaucredithistoryobtained_co' )}}
    -- DBT INCREMENTAL SENTENCE

    {% if is_incremental() %}
        WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
        AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
    {% endif %}

)
SELECT
    -- ITEM FIELDS
    CONCAT('EID_',event_id,'_IPI_',ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A')) AS surrogate_key,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A') AS item_pseudo_idx,
    -- MANDATORY FIELDS
    event_name_original,
    event_name,
    event_id,
    application_id,
    client_id,
    'json_tmp.commercial.bankAccounts._ARRAY_' AS array_parent_path,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    -- SPECIAL DOUBLE ARRAYS - ITEM ARRAYS
    -- ITEM INNER FIELDS
    item.obligationType AS bureau_bank_account_obligationType,
    NULLIF(TRIM(item.entityType),'') AS bureau_bank_account_entityType,
    NULLIF(TRIM(item.pastMonthlyBehaviour),'') AS bureau_bank_account_pastMonthlyBehaviour,
    TO_TIMESTAMP(item.originationDate) AS bureau_bank_account_originationDate,
    TO_TIMESTAMP(item.negativeInfoVisibleUntil) AS bureau_bank_account_negativeInfoVisibleUntil,
    item.entityName AS bureau_bank_account_entityName,
    NULLIF(TRIM(item.city),'') AS bureau_bank_account_city,
    item.branch AS bureau_bank_account_branch,
    item.obligationStatus AS bureau_bank_account_obligationStatus,
    item.debtorStatus AS bureau_bank_account_debtorStatus,
    TO_TIMESTAMP(item.terminationDate) AS bureau_bank_account_terminationDate,
    item.obligationNumber AS bureau_bank_account_obligationNumber,
    TO_TIMESTAMP(item.lastUpdateDate) AS bureau_bank_account_lastUpdateDate,
    item.rating AS bureau_bank_account_rating,
    NULLIF(TRIM(item.returnedChecks),'') AS bureau_bank_account_returnedChecks,
    item.contractType AS bureau_bank_account_contractType,
    item.overdraftDaysAllowed AS bureau_bank_account_overdraftDaysAllowed

FROM select_explode