
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
loanaccepted_co AS ( 
    SELECT *
    FROM bronze.loanaccepted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedco_co AS ( 
    SELECT *
    FROM bronze.loanacceptedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedsantanderco_co AS ( 
    SELECT *
    FROM bronze.loanacceptedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,client_id,loan_acceptance_detail_json,ocurred_on,
    event_name,
    event_id
    FROM loanaccepted_co
    UNION ALL
    SELECT 
        application_id,client_id,loan_acceptance_detail_json,ocurred_on,
    event_name,
    event_id
    FROM loanacceptedco_co
    UNION ALL
    SELECT 
        application_id,client_id,loan_acceptance_detail_json,ocurred_on,
    event_name,
    event_id
    FROM loanacceptedsantanderco_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,client_id,loan_acceptance_detail_json,ocurred_on,
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
this: silver.f_loan_acceptance_stage_co_logs
country: co
silver_table_name: f_loan_acceptance_stage_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'client_id', 'event_id', 'loan_acceptance_detail_json', 'ocurred_on']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'LoanAccepted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'loan_acceptance_detail_json'], 'custom_attributes': {}}, 'LoanAcceptedCO': {'stage': 'loan_acceptance_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'loan_acceptance_detail_json'], 'custom_attributes': {}}, 'loanacceptedsantanderco': {'stage': 'loan_acceptance_santander_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'loan_acceptance_detail_json'], 'custom_attributes': {}}}
events_keys: ['LoanAccepted', 'LoanAcceptedCO', 'loanacceptedsantanderco']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
