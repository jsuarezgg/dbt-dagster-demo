
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
prospectbureauincomeestimatorobtained_co AS ( 
    SELECT *
    FROM bronze.prospectbureauincomeestimatorobtained_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,income_averageSMLV,income_creditCardBalance,income_creditCardInitialApprovedAmount,income_creditCardInstallment,income_estimatedIncome,income_indebtednessCapacity,income_maximum,income_maximumSMLV,income_minimum,income_minimumSMLV,income_nonRevolvingTotalBalance,income_nonRevolvingTotalInitialApprovedAmount,income_nonRevolvingTotalInstallment,income_nonRevolvingTotalProducts,income_paymentCapacity,income_totalActiveCreditCards,income_totalActiveProducts,income_totalBalance,income_totalInitialApprovedAmount,income_totalInstallment,metadata_context_traceId,ocurred_on,
    event_name,
    event_id
    FROM prospectbureauincomeestimatorobtained_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,income_averageSMLV,income_creditCardBalance,income_creditCardInitialApprovedAmount,income_creditCardInstallment,income_estimatedIncome,income_indebtednessCapacity,income_maximum,income_maximumSMLV,income_minimum,income_minimumSMLV,income_nonRevolvingTotalBalance,income_nonRevolvingTotalInitialApprovedAmount,income_nonRevolvingTotalInstallment,income_nonRevolvingTotalProducts,income_paymentCapacity,income_totalActiveCreditCards,income_totalActiveProducts,income_totalBalance,income_totalInitialApprovedAmount,income_totalInstallment,metadata_context_traceId,ocurred_on,
    event_name,
    event_id
    FROM union_bronze 
    
)   



, final AS (
    SELECT 
        *,
        date(ocurred_on ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM union_all_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_kyc_bureau_income_estimator_co_logs
country: co
silver_table_name: f_kyc_bureau_income_estimator_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'event_id', 'income_averageSMLV', 'income_creditCardBalance', 'income_creditCardInitialApprovedAmount', 'income_creditCardInstallment', 'income_estimatedIncome', 'income_indebtednessCapacity', 'income_maximum', 'income_maximumSMLV', 'income_minimum', 'income_minimumSMLV', 'income_nonRevolvingTotalBalance', 'income_nonRevolvingTotalInitialApprovedAmount', 'income_nonRevolvingTotalInstallment', 'income_nonRevolvingTotalProducts', 'income_paymentCapacity', 'income_totalActiveCreditCards', 'income_totalActiveProducts', 'income_totalBalance', 'income_totalInitialApprovedAmount', 'income_totalInstallment', 'metadata_context_traceId', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'prospectbureauincomeestimatorobtained': {'direct_attributes': ['event_id', 'application_id', 'client_id', 'income_averageSMLV', 'income_creditCardBalance', 'income_creditCardInitialApprovedAmount', 'income_creditCardInstallment', 'income_estimatedIncome', 'income_indebtednessCapacity', 'income_maximum', 'income_maximumSMLV', 'income_minimum', 'income_minimumSMLV', 'income_nonRevolvingTotalBalance', 'income_nonRevolvingTotalInitialApprovedAmount', 'income_nonRevolvingTotalInstallment', 'income_nonRevolvingTotalProducts', 'income_paymentCapacity', 'income_totalActiveCreditCards', 'income_totalActiveProducts', 'income_totalBalance', 'income_totalInitialApprovedAmount', 'income_totalInstallment', 'metadata_context_traceId', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['prospectbureauincomeestimatorobtained']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
