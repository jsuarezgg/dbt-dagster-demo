
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
loanacceptedco_co AS ( 
    SELECT *
    FROM bronze.loanacceptedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtosendsignedfilessantanderco_co AS ( 
    SELECT *
    FROM bronze.failedtosendsignedfilessantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedbygatewaysantanderco_co AS ( 
    SELECT *
    FROM bronze.loanacceptedbygatewaysantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,allyapplicationupdated_co AS ( 
    SELECT *
    FROM bronze.allyapplicationupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectupgradedtoclient_co AS ( 
    SELECT *
    FROM bronze.prospectupgradedtoclient_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientloanaccepted_co AS ( 
    SELECT *
    FROM bronze.clientloanaccepted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,application_id,approved_amount,client_id,NULL as custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanacceptedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,approved_amount,client_id,custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,NULL as loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM failedtosendsignedfilessantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,approved_amount,client_id,custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanacceptedbygatewaysantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,NULL as approved_amount,client_id,NULL as custom_is_santander_originated,NULL as effective_annual_rate,NULL as guarantee_rate,NULL as lbl,loan_id,NULL as loan_type,ocurred_on,NULL as origination_date,store_slug,store_user_id,NULL as term,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM allyapplicationupdated_co
    UNION ALL
    SELECT 
        ally_slug,application_id,approved_amount,client_id,NULL as custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,NULL as loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectupgradedtoclient_co
    UNION ALL
    SELECT 
        ally_slug,application_id,approved_amount,client_id,NULL as custom_is_santander_originated,effective_annual_rate,NULL as guarantee_rate,lbl,loan_id,NULL as loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientloanaccepted_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,application_id,approved_amount,client_id,custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_slug,application_id,approved_amount,client_id,custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,loan_type,last_event_ocurred_on_processed as ocurred_on,origination_date,store_slug,store_user_id,term,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_originations_bnpl_co  
    WHERE 
    silver.f_originations_bnpl_co.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN ally_slug is not null then struct(ocurred_on, ally_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_slug as ally_slug,
    element_at(array_sort(array_agg(CASE WHEN approved_amount is not null then struct(ocurred_on, approved_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).approved_amount as approved_amount,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN custom_is_santander_originated is not null then struct(ocurred_on, custom_is_santander_originated) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_santander_originated as custom_is_santander_originated,
    element_at(array_sort(array_agg(CASE WHEN effective_annual_rate is not null then struct(ocurred_on, effective_annual_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).effective_annual_rate as effective_annual_rate,
    element_at(array_sort(array_agg(CASE WHEN guarantee_rate is not null then struct(ocurred_on, guarantee_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).guarantee_rate as guarantee_rate,
    element_at(array_sort(array_agg(CASE WHEN lbl is not null then struct(ocurred_on, lbl) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lbl as lbl,
    element_at(array_sort(array_agg(CASE WHEN loan_id is not null then struct(ocurred_on, loan_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_id as loan_id,
    element_at(array_sort(array_agg(CASE WHEN loan_type is not null then struct(ocurred_on, loan_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_type as loan_type,
    element_at(array_sort(array_agg(CASE WHEN origination_date is not null then struct(ocurred_on, origination_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).origination_date as origination_date,
    element_at(array_sort(array_agg(CASE WHEN store_slug is not null then struct(ocurred_on, store_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_slug as store_slug,
    element_at(array_sort(array_agg(CASE WHEN store_user_id is not null then struct(ocurred_on, store_user_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_user_id as store_user_id,
    element_at(array_sort(array_agg(CASE WHEN term is not null then struct(ocurred_on, term) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).term as term,
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
this: silver.f_originations_bnpl_co
country: co
silver_table_name: f_originations_bnpl_co
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_id', 'approved_amount', 'client_id', 'custom_is_santander_originated', 'effective_annual_rate', 'guarantee_rate', 'lbl', 'loan_id', 'loan_type', 'ocurred_on', 'origination_date', 'store_slug', 'store_user_id', 'term']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'LoanAcceptedCO': {'stage': 'loan_acceptance_co', 'direct_attributes': ['application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'loan_type', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'lbl', 'guarantee_rate', 'ocurred_on'], 'custom_attributes': {}}, 'FailedToSendSignedFilesSantanderCO': {'stage': 'loan_acceptance_santander_co', 'direct_attributes': ['application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'lbl', 'guarantee_rate', 'ocurred_on', 'custom_is_santander_originated'], 'custom_attributes': {}}, 'LoanAcceptedByGatewaySantanderCO': {'stage': 'loan_acceptance_santander_co', 'direct_attributes': ['application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'loan_type', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'lbl', 'guarantee_rate', 'ocurred_on', 'custom_is_santander_originated'], 'custom_attributes': {}}, 'AllyApplicationUpdated': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'client_id', 'loan_id', 'ally_slug', 'store_user_id', 'store_slug', 'ocurred_on'], 'custom_attributes': {}}, 'ProspectUpgradedToClient': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'guarantee_rate', 'lbl', 'ocurred_on'], 'custom_attributes': {}}, 'ClientLoanAccepted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'lbl', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['LoanAcceptedCO', 'FailedToSendSignedFilesSantanderCO', 'LoanAcceptedByGatewaySantanderCO', 'AllyApplicationUpdated', 'ProspectUpgradedToClient', 'ClientLoanAccepted']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
