
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
prospectbureaucredithistoryobtained_unnested_by_tradeline_co AS ( 
    SELECT *
    FROM bronze.prospectbureaucredithistoryobtained_unnested_by_tradeline_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,array_parent_path,bureau_tradeline_balance,bureau_tradeline_balanceShare,bureau_tradeline_branch,bureau_tradeline_city,bureau_tradeline_clauseNumberMonths,bureau_tradeline_contractNumberMonths,bureau_tradeline_contractType,bureau_tradeline_creditCardClass,bureau_tradeline_creditLineType,bureau_tradeline_creditType,bureau_tradeline_currency,bureau_tradeline_debtorStatus,bureau_tradeline_debtorType,bureau_tradeline_delinquencyLikelihood,bureau_tradeline_entityName,bureau_tradeline_entityType,bureau_tradeline_extinctionPaymentMode,bureau_tradeline_franchise,bureau_tradeline_initialApprovedAmount,bureau_tradeline_installmentAmount,bureau_tradeline_installmentsPaid,bureau_tradeline_lastUpdateDate,bureau_tradeline_maxDaysPastDue,bureau_tradeline_minMonthsClause,bureau_tradeline_negativeInfoVisibleUntil,bureau_tradeline_numberAgreedInstallments,bureau_tradeline_numberInstallmentsPastDue,bureau_tradeline_obligationNumber,bureau_tradeline_obligationStatus,bureau_tradeline_obligationStatusLastUpdateDate,bureau_tradeline_obligationType,bureau_tradeline_originationDate,bureau_tradeline_originationEntityName,bureau_tradeline_pastDueAmount,bureau_tradeline_pastMonthlyBehaviour,bureau_tradeline_pastMonthlyBehaviours,bureau_tradeline_paymentDate,bureau_tradeline_paymentStatus,bureau_tradeline_paymentStatusLastUpdateDate,bureau_tradeline_paymentType,bureau_tradeline_periodicity,bureau_tradeline_plasticStatus,bureau_tradeline_plasticStatusLastUpdateDate,bureau_tradeline_rating,bureau_tradeline_refinanceType,bureau_tradeline_refinanced,bureau_tradeline_refinancedDate,bureau_tradeline_returnedChecks,bureau_tradeline_terminationDate,bureau_tradeline_timesRefinanced,bureau_tradeline_validity,bureau_tradeline_warrantyCoverage,bureau_tradeline_warrantyType,client_id,item_pseudo_idx,ocurred_on,surrogate_key,
    event_name,
    event_id
    FROM prospectbureaucredithistoryobtained_unnested_by_tradeline_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,array_parent_path,bureau_tradeline_balance,bureau_tradeline_balanceShare,bureau_tradeline_branch,bureau_tradeline_city,bureau_tradeline_clauseNumberMonths,bureau_tradeline_contractNumberMonths,bureau_tradeline_contractType,bureau_tradeline_creditCardClass,bureau_tradeline_creditLineType,bureau_tradeline_creditType,bureau_tradeline_currency,bureau_tradeline_debtorStatus,bureau_tradeline_debtorType,bureau_tradeline_delinquencyLikelihood,bureau_tradeline_entityName,bureau_tradeline_entityType,bureau_tradeline_extinctionPaymentMode,bureau_tradeline_franchise,bureau_tradeline_initialApprovedAmount,bureau_tradeline_installmentAmount,bureau_tradeline_installmentsPaid,bureau_tradeline_lastUpdateDate,bureau_tradeline_maxDaysPastDue,bureau_tradeline_minMonthsClause,bureau_tradeline_negativeInfoVisibleUntil,bureau_tradeline_numberAgreedInstallments,bureau_tradeline_numberInstallmentsPastDue,bureau_tradeline_obligationNumber,bureau_tradeline_obligationStatus,bureau_tradeline_obligationStatusLastUpdateDate,bureau_tradeline_obligationType,bureau_tradeline_originationDate,bureau_tradeline_originationEntityName,bureau_tradeline_pastDueAmount,bureau_tradeline_pastMonthlyBehaviour,bureau_tradeline_pastMonthlyBehaviours,bureau_tradeline_paymentDate,bureau_tradeline_paymentStatus,bureau_tradeline_paymentStatusLastUpdateDate,bureau_tradeline_paymentType,bureau_tradeline_periodicity,bureau_tradeline_plasticStatus,bureau_tradeline_plasticStatusLastUpdateDate,bureau_tradeline_rating,bureau_tradeline_refinanceType,bureau_tradeline_refinanced,bureau_tradeline_refinancedDate,bureau_tradeline_returnedChecks,bureau_tradeline_terminationDate,bureau_tradeline_timesRefinanced,bureau_tradeline_validity,bureau_tradeline_warrantyCoverage,bureau_tradeline_warrantyType,client_id,item_pseudo_idx,ocurred_on,surrogate_key,
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
this: silver.f_kyc_bureau_credit_history_tradelines_co_logs
country: co
silver_table_name: f_kyc_bureau_credit_history_tradelines_co_logs
table_pk_fields: ['surrogate_key']
table_pk_amount: 1
fields_direct: ['application_id', 'array_parent_path', 'bureau_tradeline_balance', 'bureau_tradeline_balanceShare', 'bureau_tradeline_branch', 'bureau_tradeline_city', 'bureau_tradeline_clauseNumberMonths', 'bureau_tradeline_contractNumberMonths', 'bureau_tradeline_contractType', 'bureau_tradeline_creditCardClass', 'bureau_tradeline_creditLineType', 'bureau_tradeline_creditType', 'bureau_tradeline_currency', 'bureau_tradeline_debtorStatus', 'bureau_tradeline_debtorType', 'bureau_tradeline_delinquencyLikelihood', 'bureau_tradeline_entityName', 'bureau_tradeline_entityType', 'bureau_tradeline_extinctionPaymentMode', 'bureau_tradeline_franchise', 'bureau_tradeline_initialApprovedAmount', 'bureau_tradeline_installmentAmount', 'bureau_tradeline_installmentsPaid', 'bureau_tradeline_lastUpdateDate', 'bureau_tradeline_maxDaysPastDue', 'bureau_tradeline_minMonthsClause', 'bureau_tradeline_negativeInfoVisibleUntil', 'bureau_tradeline_numberAgreedInstallments', 'bureau_tradeline_numberInstallmentsPastDue', 'bureau_tradeline_obligationNumber', 'bureau_tradeline_obligationStatus', 'bureau_tradeline_obligationStatusLastUpdateDate', 'bureau_tradeline_obligationType', 'bureau_tradeline_originationDate', 'bureau_tradeline_originationEntityName', 'bureau_tradeline_pastDueAmount', 'bureau_tradeline_pastMonthlyBehaviour', 'bureau_tradeline_pastMonthlyBehaviours', 'bureau_tradeline_paymentDate', 'bureau_tradeline_paymentStatus', 'bureau_tradeline_paymentStatusLastUpdateDate', 'bureau_tradeline_paymentType', 'bureau_tradeline_periodicity', 'bureau_tradeline_plasticStatus', 'bureau_tradeline_plasticStatusLastUpdateDate', 'bureau_tradeline_rating', 'bureau_tradeline_refinanceType', 'bureau_tradeline_refinanced', 'bureau_tradeline_refinancedDate', 'bureau_tradeline_returnedChecks', 'bureau_tradeline_terminationDate', 'bureau_tradeline_timesRefinanced', 'bureau_tradeline_validity', 'bureau_tradeline_warrantyCoverage', 'bureau_tradeline_warrantyType', 'client_id', 'event_id', 'item_pseudo_idx', 'ocurred_on', 'surrogate_key']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'prospectbureaucredithistoryobtained_unnested_by_tradeline': {'direct_attributes': ['surrogate_key', 'item_pseudo_idx', 'event_id', 'application_id', 'client_id', 'array_parent_path', 'bureau_tradeline_pastMonthlyBehaviours', 'bureau_tradeline_balance', 'bureau_tradeline_balanceShare', 'bureau_tradeline_branch', 'bureau_tradeline_city', 'bureau_tradeline_clauseNumberMonths', 'bureau_tradeline_contractNumberMonths', 'bureau_tradeline_contractType', 'bureau_tradeline_creditCardClass', 'bureau_tradeline_creditLineType', 'bureau_tradeline_creditType', 'bureau_tradeline_currency', 'bureau_tradeline_debtorStatus', 'bureau_tradeline_debtorType', 'bureau_tradeline_delinquencyLikelihood', 'bureau_tradeline_entityName', 'bureau_tradeline_entityType', 'bureau_tradeline_extinctionPaymentMode', 'bureau_tradeline_franchise', 'bureau_tradeline_initialApprovedAmount', 'bureau_tradeline_installmentAmount', 'bureau_tradeline_installmentsPaid', 'bureau_tradeline_lastUpdateDate', 'bureau_tradeline_maxDaysPastDue', 'bureau_tradeline_minMonthsClause', 'bureau_tradeline_negativeInfoVisibleUntil', 'bureau_tradeline_numberAgreedInstallments', 'bureau_tradeline_numberInstallmentsPastDue', 'bureau_tradeline_obligationNumber', 'bureau_tradeline_obligationStatus', 'bureau_tradeline_obligationStatusLastUpdateDate', 'bureau_tradeline_obligationType', 'bureau_tradeline_originationDate', 'bureau_tradeline_originationEntityName', 'bureau_tradeline_pastDueAmount', 'bureau_tradeline_pastMonthlyBehaviour', 'bureau_tradeline_paymentDate', 'bureau_tradeline_paymentStatus', 'bureau_tradeline_paymentStatusLastUpdateDate', 'bureau_tradeline_paymentType', 'bureau_tradeline_periodicity', 'bureau_tradeline_plasticStatus', 'bureau_tradeline_plasticStatusLastUpdateDate', 'bureau_tradeline_rating', 'bureau_tradeline_refinanceType', 'bureau_tradeline_refinanced', 'bureau_tradeline_refinancedDate', 'bureau_tradeline_returnedChecks', 'bureau_tradeline_terminationDate', 'bureau_tradeline_timesRefinanced', 'bureau_tradeline_validity', 'bureau_tradeline_warrantyCoverage', 'bureau_tradeline_warrantyType', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['prospectbureaucredithistoryobtained_unnested_by_tradeline']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
