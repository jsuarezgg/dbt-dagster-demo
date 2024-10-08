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
--raw_modeling.prospectbureaucredithistoryobtained_co
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
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.balance AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_balance,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.closedAccounts AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_closedAccounts,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.creditUtilizationRatio AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_creditUtilizationRatio,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.initialApprovedAmount AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_initialApprovedAmount,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.installmentAmount AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_installmentAmount,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.maximumDelinquency AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_maximumDelinquency,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.openedAccounts AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_openedAccounts,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.rating AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_rating,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.score AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_score,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.totalClosedAccounts AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_totalClosedAccounts,
    json_tmp.commercial.commercialSummaryExperian.balanceEvolution.averageAnalysis.totalOpenedAccounts AS commercial_commercialSummaryExperian_balanceEvolution_averageAnalysis_totalOpenedAccounts,
    json_tmp.commercial.commercialSummaryExperian.summary.balance.delinquent30Balance AS commercial_commercialSummaryExperian_summary_balance_delinquent30Balance,
    json_tmp.commercial.commercialSummaryExperian.summary.balance.delinquent60Balance AS commercial_commercialSummaryExperian_summary_balance_delinquent60Balance,
    json_tmp.commercial.commercialSummaryExperian.summary.balance.delinquent90Balance AS commercial_commercialSummaryExperian_summary_balance_delinquent90Balance,
    json_tmp.commercial.commercialSummaryExperian.summary.balance.delinquentBalance AS commercial_commercialSummaryExperian_summary_balance_delinquentBalance,
    json_tmp.commercial.commercialSummaryExperian.summary.balance.higherBalance AS commercial_commercialSummaryExperian_summary_balance_higherBalance,
    json_tmp.commercial.commercialSummaryExperian.summary.balance.installmentAmount AS commercial_commercialSummaryExperian_summary_balance_installmentAmount,
    json_tmp.commercial.commercialSummaryExperian.summary.balance.totalBalance AS commercial_commercialSummaryExperian_summary_balance_totalBalance,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.claims AS commercial_commercialSummaryExperian_summary_principal_claims,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.closedAccounts AS commercial_commercialSummaryExperian_summary_principal_closedAccounts,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.closedAccountsAHOCCB AS commercial_commercialSummaryExperian_summary_principal_closedAccountsAHOCCB,
    TO_TIMESTAMP(json_tmp.commercial.commercialSummaryExperian.summary.principal.customerSince) AS commercial_commercialSummaryExperian_summary_principal_customerSince,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.disagreementsToDate AS commercial_commercialSummaryExperian_summary_principal_disagreementsToDate,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.lastSixMonthsQueries AS commercial_commercialSummaryExperian_summary_principal_lastSixMonthsQueries,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.negativeHistoryLastTwelveMonths AS commercial_commercialSummaryExperian_summary_principal_negativeHistoryLastTwelveMonths,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.negativeOpenAccounts AS commercial_commercialSummaryExperian_summary_principal_negativeOpenAccounts,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.openAccounts AS commercial_commercialSummaryExperian_summary_principal_openAccounts,
    json_tmp.commercial.commercialSummaryExperian.summary.principal.openAccountsAHOCCB AS commercial_commercialSummaryExperian_summary_principal_openAccountsAHOCCB,
    json_tmp.commercial.incomeEstimator.averageSMLV AS commercial_incomeEstimator_averageSMLV,
    json_tmp.commercial.incomeEstimator.creditCardBalance AS commercial_incomeEstimator_creditCardBalance,
    json_tmp.commercial.incomeEstimator.creditCardInitialApprovedAmount AS commercial_incomeEstimator_creditCardInitialApprovedAmount,
    json_tmp.commercial.incomeEstimator.creditCardInstallment AS commercial_incomeEstimator_creditCardInstallment,
    json_tmp.commercial.incomeEstimator.estimatedIncome AS commercial_incomeEstimator_estimatedIncome,
    json_tmp.commercial.incomeEstimator.indebtednessCapacity AS commercial_incomeEstimator_indebtednessCapacity,
    json_tmp.commercial.incomeEstimator.maximum AS commercial_incomeEstimator_maximum,
    json_tmp.commercial.incomeEstimator.maximumSMLV AS commercial_incomeEstimator_maximumSMLV,
    json_tmp.commercial.incomeEstimator.minimum AS commercial_incomeEstimator_minimum,
    json_tmp.commercial.incomeEstimator.minimumSMLV AS commercial_incomeEstimator_minimumSMLV,
    json_tmp.commercial.incomeEstimator.nonRevolvingTotalBalance AS commercial_incomeEstimator_nonRevolvingTotalBalance,
    json_tmp.commercial.incomeEstimator.nonRevolvingTotalInitialApprovedAmount AS commercial_incomeEstimator_nonRevolvingTotalInitialApprovedAmount,
    json_tmp.commercial.incomeEstimator.nonRevolvingTotalInstallment AS commercial_incomeEstimator_nonRevolvingTotalInstallment,
    json_tmp.commercial.incomeEstimator.nonRevolvingTotalProducts AS commercial_incomeEstimator_nonRevolvingTotalProducts,
    json_tmp.commercial.incomeEstimator.paymentCapacity AS commercial_incomeEstimator_paymentCapacity,
    json_tmp.commercial.incomeEstimator.totalActiveCreditCards AS commercial_incomeEstimator_totalActiveCreditCards,
    json_tmp.commercial.incomeEstimator.totalActiveProducts AS commercial_incomeEstimator_totalActiveProducts,
    json_tmp.commercial.incomeEstimator.totalBalance AS commercial_incomeEstimator_totalBalance,
    json_tmp.commercial.incomeEstimator.totalInitialApprovedAmount AS commercial_incomeEstimator_totalInitialApprovedAmount,
    json_tmp.commercial.incomeEstimator.totalInstallment AS commercial_incomeEstimator_totalInstallment,
    json_tmp.commercial.incomeValidator.healthEntity AS commercial_incomeValidator_healthEntity,
    json_tmp.commercial.incomeValidator.pensionFundName AS commercial_incomeValidator_pensionFundName,
    TO_TIMESTAMP(json_tmp.commercial.indebtednessSummary.quarter1.date) AS commercial_indebtednessSummary_quarter1_date,
    json_tmp.commercial.indebtednessSummary.quarter1.numberOfEntities AS commercial_indebtednessSummary_quarter1_numberOfEntities,
    json_tmp.commercial.indebtednessSummary.quarter1.numberOfPurchases AS commercial_indebtednessSummary_quarter1_numberOfPurchases,
    json_tmp.commercial.indebtednessSummary.quarter1.numberOfRefinanced AS commercial_indebtednessSummary_quarter1_numberOfRefinanced,
    json_tmp.commercial.indebtednessSummary.quarter1.numberOfWriteOffs AS commercial_indebtednessSummary_quarter1_numberOfWriteOffs,
    TO_TIMESTAMP(json_tmp.commercial.indebtednessSummary.quarter2.date) AS commercial_indebtednessSummary_quarter2_date,
    json_tmp.commercial.indebtednessSummary.quarter2.numberOfEntities AS commercial_indebtednessSummary_quarter2_numberOfEntities,
    json_tmp.commercial.indebtednessSummary.quarter2.numberOfPurchases AS commercial_indebtednessSummary_quarter2_numberOfPurchases,
    json_tmp.commercial.indebtednessSummary.quarter2.numberOfRefinanced AS commercial_indebtednessSummary_quarter2_numberOfRefinanced,
    json_tmp.commercial.indebtednessSummary.quarter2.numberOfWriteOffs AS commercial_indebtednessSummary_quarter2_numberOfWriteOffs,
    TO_TIMESTAMP(json_tmp.commercial.indebtednessSummary.quarter3.date) AS commercial_indebtednessSummary_quarter3_date,
    json_tmp.commercial.indebtednessSummary.quarter3.numberOfEntities AS commercial_indebtednessSummary_quarter3_numberOfEntities,
    json_tmp.commercial.indebtednessSummary.quarter3.numberOfPurchases AS commercial_indebtednessSummary_quarter3_numberOfPurchases,
    json_tmp.commercial.indebtednessSummary.quarter3.numberOfRefinanced AS commercial_indebtednessSummary_quarter3_numberOfRefinanced,
    json_tmp.commercial.indebtednessSummary.quarter3.numberOfWriteOffs AS commercial_indebtednessSummary_quarter3_numberOfWriteOffs,
    json_tmp.commercial.provider AS commercial_provider,
    json_tmp.metadata.context.traceId AS metadata_context_traceId,
    -- CUSTOM ATTRIBUTES
    'V1' AS custom_kyc_event_version
    -- CAST(ocurred_on AS TIMESTAMP) AS prospectbureaucredithistoryobtained_co_at -- To store it as a standalone column, when needed
FROM  {{source('raw_modeling', 'prospectbureaucredithistoryobtained_co' )}}
-- DBT INCREMENTAL SENTENCE

{% if is_incremental() %}
    WHERE dt BETWEEN (to_date("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_date("{{ var('end_date') }}")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("{{ var('start_date') }}"- INTERVAL "{{var('incremental_slack_time_in_days')}}" DAY)) AND to_timestamp("{{ var('end_date')}}")
{% endif %}