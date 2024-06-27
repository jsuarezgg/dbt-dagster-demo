
--raw_modeling.prospectbureauincomeestimatorobtained_co
SELECT
    -- MANDATORY FIELDS
    json_tmp.eventType AS event_name_original,
    reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
    json_tmp.eventId AS event_id,
    CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
    dt AS ocurred_on_date,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at,
    -- MAPPED FIELDS
    json_tmp.applicationId AS application_id,
    json_tmp.prospectId AS client_id,
    json_tmp.income.averageSMLV AS income_averageSMLV,
    json_tmp.income.creditCardBalance AS income_creditCardBalance,
    json_tmp.income.creditCardInitialApprovedAmount AS income_creditCardInitialApprovedAmount,
    json_tmp.income.creditCardInstallment AS income_creditCardInstallment,
    json_tmp.income.estimatedIncome AS income_estimatedIncome,
    json_tmp.income.indebtednessCapacity AS income_indebtednessCapacity,
    json_tmp.income.maximum AS income_maximum,
    json_tmp.income.maximumSMLV AS income_maximumSMLV,
    json_tmp.income.minimum AS income_minimum,
    json_tmp.income.minimumSMLV AS income_minimumSMLV,
    json_tmp.income.nonRevolvingTotalBalance AS income_nonRevolvingTotalBalance,
    json_tmp.income.nonRevolvingTotalInitialApprovedAmount AS income_nonRevolvingTotalInitialApprovedAmount,
    json_tmp.income.nonRevolvingTotalInstallment AS income_nonRevolvingTotalInstallment,
    json_tmp.income.nonRevolvingTotalProducts AS income_nonRevolvingTotalProducts,
    json_tmp.income.paymentCapacity AS income_paymentCapacity,
    json_tmp.income.totalActiveCreditCards AS income_totalActiveCreditCards,
    json_tmp.income.totalActiveProducts AS income_totalActiveProducts,
    json_tmp.income.totalBalance AS income_totalBalance,
    json_tmp.income.totalInitialApprovedAmount AS income_totalInitialApprovedAmount,
    json_tmp.income.totalInstallment AS income_totalInstallment,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'V1' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS prospectbureauincomeestimatorobtained_co_at -- To store it as a standalone column, when needed
FROM  raw_modeling.prospectbureauincomeestimatorobtained_co
-- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
