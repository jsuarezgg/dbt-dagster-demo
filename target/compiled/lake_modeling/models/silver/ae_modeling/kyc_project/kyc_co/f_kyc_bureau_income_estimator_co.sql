
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
        application_id,client_id,income_averageSMLV,income_creditCardBalance,income_creditCardInitialApprovedAmount,income_creditCardInstallment,income_estimatedIncome,income_indebtednessCapacity,income_maximum,income_maximumSMLV,income_minimum,income_minimumSMLV,income_nonRevolvingTotalBalance,income_nonRevolvingTotalInitialApprovedAmount,income_nonRevolvingTotalInstallment,income_nonRevolvingTotalProducts,income_paymentCapacity,income_totalActiveCreditCards,income_totalActiveProducts,income_totalBalance,income_totalInitialApprovedAmount,income_totalInstallment,metadata_context_traceId,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectbureauincomeestimatorobtained_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,income_averageSMLV,income_creditCardBalance,income_creditCardInitialApprovedAmount,income_creditCardInstallment,income_estimatedIncome,income_indebtednessCapacity,income_maximum,income_maximumSMLV,income_minimum,income_minimumSMLV,income_nonRevolvingTotalBalance,income_nonRevolvingTotalInitialApprovedAmount,income_nonRevolvingTotalInstallment,income_nonRevolvingTotalProducts,income_paymentCapacity,income_totalActiveCreditCards,income_totalActiveProducts,income_totalBalance,income_totalInitialApprovedAmount,income_totalInstallment,metadata_context_traceId,ocurred_on,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,client_id,income_averageSMLV,income_creditCardBalance,income_creditCardInitialApprovedAmount,income_creditCardInstallment,income_estimatedIncome,income_indebtednessCapacity,income_maximum,income_maximumSMLV,income_minimum,income_minimumSMLV,income_nonRevolvingTotalBalance,income_nonRevolvingTotalInitialApprovedAmount,income_nonRevolvingTotalInstallment,income_nonRevolvingTotalProducts,income_paymentCapacity,income_totalActiveCreditCards,income_totalActiveProducts,income_totalBalance,income_totalInitialApprovedAmount,income_totalInstallment,metadata_context_traceId,last_event_ocurred_on_processed as ocurred_on,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_kyc_bureau_income_estimator_co  
    WHERE 
    silver.f_kyc_bureau_income_estimator_co.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN income_averageSMLV is not null then struct(ocurred_on, income_averageSMLV) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_averageSMLV as income_averageSMLV,
    element_at(array_sort(array_agg(CASE WHEN income_creditCardBalance is not null then struct(ocurred_on, income_creditCardBalance) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_creditCardBalance as income_creditCardBalance,
    element_at(array_sort(array_agg(CASE WHEN income_creditCardInitialApprovedAmount is not null then struct(ocurred_on, income_creditCardInitialApprovedAmount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_creditCardInitialApprovedAmount as income_creditCardInitialApprovedAmount,
    element_at(array_sort(array_agg(CASE WHEN income_creditCardInstallment is not null then struct(ocurred_on, income_creditCardInstallment) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_creditCardInstallment as income_creditCardInstallment,
    element_at(array_sort(array_agg(CASE WHEN income_estimatedIncome is not null then struct(ocurred_on, income_estimatedIncome) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_estimatedIncome as income_estimatedIncome,
    element_at(array_sort(array_agg(CASE WHEN income_indebtednessCapacity is not null then struct(ocurred_on, income_indebtednessCapacity) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_indebtednessCapacity as income_indebtednessCapacity,
    element_at(array_sort(array_agg(CASE WHEN income_maximum is not null then struct(ocurred_on, income_maximum) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_maximum as income_maximum,
    element_at(array_sort(array_agg(CASE WHEN income_maximumSMLV is not null then struct(ocurred_on, income_maximumSMLV) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_maximumSMLV as income_maximumSMLV,
    element_at(array_sort(array_agg(CASE WHEN income_minimum is not null then struct(ocurred_on, income_minimum) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_minimum as income_minimum,
    element_at(array_sort(array_agg(CASE WHEN income_minimumSMLV is not null then struct(ocurred_on, income_minimumSMLV) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_minimumSMLV as income_minimumSMLV,
    element_at(array_sort(array_agg(CASE WHEN income_nonRevolvingTotalBalance is not null then struct(ocurred_on, income_nonRevolvingTotalBalance) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_nonRevolvingTotalBalance as income_nonRevolvingTotalBalance,
    element_at(array_sort(array_agg(CASE WHEN income_nonRevolvingTotalInitialApprovedAmount is not null then struct(ocurred_on, income_nonRevolvingTotalInitialApprovedAmount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_nonRevolvingTotalInitialApprovedAmount as income_nonRevolvingTotalInitialApprovedAmount,
    element_at(array_sort(array_agg(CASE WHEN income_nonRevolvingTotalInstallment is not null then struct(ocurred_on, income_nonRevolvingTotalInstallment) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_nonRevolvingTotalInstallment as income_nonRevolvingTotalInstallment,
    element_at(array_sort(array_agg(CASE WHEN income_nonRevolvingTotalProducts is not null then struct(ocurred_on, income_nonRevolvingTotalProducts) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_nonRevolvingTotalProducts as income_nonRevolvingTotalProducts,
    element_at(array_sort(array_agg(CASE WHEN income_paymentCapacity is not null then struct(ocurred_on, income_paymentCapacity) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_paymentCapacity as income_paymentCapacity,
    element_at(array_sort(array_agg(CASE WHEN income_totalActiveCreditCards is not null then struct(ocurred_on, income_totalActiveCreditCards) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_totalActiveCreditCards as income_totalActiveCreditCards,
    element_at(array_sort(array_agg(CASE WHEN income_totalActiveProducts is not null then struct(ocurred_on, income_totalActiveProducts) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_totalActiveProducts as income_totalActiveProducts,
    element_at(array_sort(array_agg(CASE WHEN income_totalBalance is not null then struct(ocurred_on, income_totalBalance) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_totalBalance as income_totalBalance,
    element_at(array_sort(array_agg(CASE WHEN income_totalInitialApprovedAmount is not null then struct(ocurred_on, income_totalInitialApprovedAmount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_totalInitialApprovedAmount as income_totalInitialApprovedAmount,
    element_at(array_sort(array_agg(CASE WHEN income_totalInstallment is not null then struct(ocurred_on, income_totalInstallment) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).income_totalInstallment as income_totalInstallment,
    element_at(array_sort(array_agg(CASE WHEN metadata_context_traceId is not null then struct(ocurred_on, metadata_context_traceId) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).metadata_context_traceId as metadata_context_traceId,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    application_id
                       
           )


, final AS (
    SELECT 
        *,
        date(last_event_ocurred_on_processed ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM grouped_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_kyc_bureau_income_estimator_co
country: co
silver_table_name: f_kyc_bureau_income_estimator_co
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'income_averageSMLV', 'income_creditCardBalance', 'income_creditCardInitialApprovedAmount', 'income_creditCardInstallment', 'income_estimatedIncome', 'income_indebtednessCapacity', 'income_maximum', 'income_maximumSMLV', 'income_minimum', 'income_minimumSMLV', 'income_nonRevolvingTotalBalance', 'income_nonRevolvingTotalInitialApprovedAmount', 'income_nonRevolvingTotalInstallment', 'income_nonRevolvingTotalProducts', 'income_paymentCapacity', 'income_totalActiveCreditCards', 'income_totalActiveProducts', 'income_totalBalance', 'income_totalInitialApprovedAmount', 'income_totalInstallment', 'metadata_context_traceId', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'prospectbureauincomeestimatorobtained': {'direct_attributes': ['application_id', 'client_id', 'income_averageSMLV', 'income_creditCardBalance', 'income_creditCardInitialApprovedAmount', 'income_creditCardInstallment', 'income_estimatedIncome', 'income_indebtednessCapacity', 'income_maximum', 'income_maximumSMLV', 'income_minimum', 'income_minimumSMLV', 'income_nonRevolvingTotalBalance', 'income_nonRevolvingTotalInitialApprovedAmount', 'income_nonRevolvingTotalInstallment', 'income_nonRevolvingTotalProducts', 'income_paymentCapacity', 'income_totalActiveCreditCards', 'income_totalActiveProducts', 'income_totalBalance', 'income_totalInitialApprovedAmount', 'income_totalInstallment', 'metadata_context_traceId', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['prospectbureauincomeestimatorobtained']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
