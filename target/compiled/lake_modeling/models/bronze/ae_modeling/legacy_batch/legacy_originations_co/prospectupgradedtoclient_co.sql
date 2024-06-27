


--raw_modeling.prospectupgradedtoclient_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
    to_date(json_tmp.ocurredOn) AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
    coalesce(json_tmp.applicationId.id,
             json_tmp.applicationId.value,
             json_tmp.metadata.context.traceId) AS application_id,
    json_tmp.metadata.context.allyId AS ally_slug,
    json_tmp.clientInfo.email AS application_email,
    json_tmp.loan.approvedAmount AS approved_amount,
    coalesce(json_tmp.clientInfo.prospectId.id,
             json_tmp.metadata.context.clientId) AS client_id,
    json_tmp.loan.guaranteePolicy.totalRate.value AS guarantee_rate,
    json_tmp.loan.effectiveAnnualRate AS effective_annual_rate,
    json_tmp.loan.isLowBalanceV2.value AS lbl,
    json_tmp.loan.loanId.id AS loan_id,
    to_date(json_tmp.ocurredOn) AS origination_date,
    cast(false AS BOOLEAN) AS returning_client,
    json_tmp.metadata.context.storeId AS store_slug,
    coalesce(json_tmp.metadata.context.storeUserId,
             json_tmp.metadata.context.userId) AS store_user_id,
    json_tmp.loan.term AS term,
    json_tmp.loan.ownership AS ownership,
    CAST(json_tmp.loan.firstPaymentDate AS TIMESTAMP) AS first_payment_date,
    json_tmp.clientInfo.prospectInfo.personalInfo.idType AS id_type,
    json_tmp.clientInfo.prospectInfo.personalInfo.idNumber AS id_number,
    json_tmp.totalAddiCupo as client_max_exposure,
    -- json_tmp.clientInfo.prospectInfo.commercialInfo.scores.score as credit_score, MAL MAPEADO
    -- json_tmp.clientInfo.prospectInfo.commercialInfo.scores.type as credit_score_name, MAL MAPEADO
    -- json_tmp.clientInfo.prospectInfo.commercialInfo.scores.name.value as credit_score_product, MAL MAPEADO
    -- json_tmp.groupName as group_name,
    -- CUSTOM ATTRIBUTES
    md5(cast(concat(coalesce(cast(event_id as 
    string
), ''), '-', coalesce(cast(json_tmp.loan.loanId.id as 
    string
), '')) as 
    string
)) AS surrogate_key
-- DBT SOURCE REFERENCE
from raw_modeling.prospectupgradedtoclient_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
