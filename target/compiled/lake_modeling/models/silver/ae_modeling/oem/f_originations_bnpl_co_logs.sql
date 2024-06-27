
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
        ally_slug,application_id,approved_amount,client_id,NULL as custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,
    event_name,
    event_id
    FROM loanacceptedco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,approved_amount,client_id,custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,NULL as loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,
    event_name,
    event_id
    FROM failedtosendsignedfilessantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,approved_amount,client_id,custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,
    event_name,
    event_id
    FROM loanacceptedbygatewaysantanderco_co
    UNION ALL
    SELECT 
        ally_slug,application_id,NULL as approved_amount,client_id,NULL as custom_is_santander_originated,NULL as effective_annual_rate,NULL as guarantee_rate,NULL as lbl,loan_id,NULL as loan_type,ocurred_on,NULL as origination_date,store_slug,store_user_id,NULL as term,
    event_name,
    event_id
    FROM allyapplicationupdated_co
    UNION ALL
    SELECT 
        ally_slug,application_id,approved_amount,client_id,NULL as custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,NULL as loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,
    event_name,
    event_id
    FROM prospectupgradedtoclient_co
    UNION ALL
    SELECT 
        ally_slug,application_id,approved_amount,client_id,NULL as custom_is_santander_originated,effective_annual_rate,NULL as guarantee_rate,lbl,loan_id,NULL as loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,
    event_name,
    event_id
    FROM clientloanaccepted_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,application_id,approved_amount,client_id,custom_is_santander_originated,effective_annual_rate,guarantee_rate,lbl,loan_id,loan_type,ocurred_on,origination_date,store_slug,store_user_id,term,
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
this: silver.f_originations_bnpl_co_logs
country: co
silver_table_name: f_originations_bnpl_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_id', 'approved_amount', 'client_id', 'custom_is_santander_originated', 'effective_annual_rate', 'event_id', 'guarantee_rate', 'lbl', 'loan_id', 'loan_type', 'ocurred_on', 'origination_date', 'store_slug', 'store_user_id', 'term']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'LoanAcceptedCO': {'stage': 'loan_acceptance_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'loan_type', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'lbl', 'guarantee_rate', 'ocurred_on'], 'custom_attributes': {}}, 'FailedToSendSignedFilesSantanderCO': {'stage': 'loan_acceptance_santander_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'lbl', 'guarantee_rate', 'ocurred_on', 'custom_is_santander_originated'], 'custom_attributes': {}}, 'LoanAcceptedByGatewaySantanderCO': {'stage': 'loan_acceptance_santander_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'loan_type', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'lbl', 'guarantee_rate', 'ocurred_on', 'custom_is_santander_originated'], 'custom_attributes': {}}, 'AllyApplicationUpdated': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'loan_id', 'ally_slug', 'store_user_id', 'store_slug', 'ocurred_on'], 'custom_attributes': {}}, 'ProspectUpgradedToClient': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'guarantee_rate', 'lbl', 'ocurred_on'], 'custom_attributes': {}}, 'ClientLoanAccepted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'loan_id', 'origination_date', 'approved_amount', 'ally_slug', 'store_slug', 'store_user_id', 'term', 'effective_annual_rate', 'lbl', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['LoanAcceptedCO', 'FailedToSendSignedFilesSantanderCO', 'LoanAcceptedByGatewaySantanderCO', 'AllyApplicationUpdated', 'ProspectUpgradedToClient', 'ClientLoanAccepted']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
