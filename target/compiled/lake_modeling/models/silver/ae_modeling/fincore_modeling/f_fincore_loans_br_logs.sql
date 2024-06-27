
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
clientloansstatusupdatedv2_unnested_by_loan_id_br AS ( 
    SELECT *
    FROM bronze.clientloansstatusupdatedv2_unnested_by_loan_id_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectloanacceptedbybankinglicensepartner_br AS ( 
    SELECT *
    FROM bronze.prospectloanacceptedbybankinglicensepartner_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedbybankinglicensepartnerbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptedbybankinglicensepartnerbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedwasnotsenttobankinglicensepartnerbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptedwasnotsenttobankinglicensepartnerbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)



-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    
    SELECT 
        applicable_rate,NULL as application_id,approved_amount,client_id,current_installment_amount,custom_fincore_included,days_past_due,delinquency_balance,NULL as down_payment_amount,effective_annual_rate,first_payment_date,full_payment,guarantee_overdue,initial_installment_amount,interest_on_overdue_principal,interest_overdue,is_fully_paid,late_fees,loan_id,NULL as loan_type,min_payment,months_on_book,ocurred_on,origination_date,paid_installments,principal_overdue,state,surrogate_key,term,total_interest_paid,total_late_fees_condoned,total_late_fees_paid,total_payment_applied,total_principal_paid,unpaid_guarantee,unpaid_principal,
    event_name,
    event_id
    FROM clientloansstatusupdatedv2_unnested_by_loan_id_br
    UNION ALL
    SELECT 
        NULL as applicable_rate,application_id,approved_amount,client_id,NULL as current_installment_amount,NULL as custom_fincore_included,NULL as days_past_due,NULL as delinquency_balance,NULL as down_payment_amount,effective_annual_rate,NULL as first_payment_date,NULL as full_payment,NULL as guarantee_overdue,NULL as initial_installment_amount,NULL as interest_on_overdue_principal,NULL as interest_overdue,NULL as is_fully_paid,NULL as late_fees,loan_id,NULL as loan_type,NULL as min_payment,NULL as months_on_book,ocurred_on,origination_date,NULL as paid_installments,NULL as principal_overdue,NULL as state,surrogate_key,term,NULL as total_interest_paid,NULL as total_late_fees_condoned,NULL as total_late_fees_paid,NULL as total_payment_applied,NULL as total_principal_paid,NULL as unpaid_guarantee,NULL as unpaid_principal,
    event_name,
    event_id
    FROM prospectloanacceptedbybankinglicensepartner_br
    UNION ALL
    SELECT 
        NULL as applicable_rate,application_id,approved_amount,client_id,NULL as current_installment_amount,NULL as custom_fincore_included,NULL as days_past_due,NULL as delinquency_balance,down_payment_amount,effective_annual_rate,NULL as first_payment_date,NULL as full_payment,NULL as guarantee_overdue,NULL as initial_installment_amount,NULL as interest_on_overdue_principal,NULL as interest_overdue,NULL as is_fully_paid,NULL as late_fees,loan_id,loan_type,NULL as min_payment,NULL as months_on_book,ocurred_on,origination_date,NULL as paid_installments,NULL as principal_overdue,NULL as state,surrogate_key,term,NULL as total_interest_paid,NULL as total_late_fees_condoned,NULL as total_late_fees_paid,NULL as total_payment_applied,NULL as total_principal_paid,NULL as unpaid_guarantee,NULL as unpaid_principal,
    event_name,
    event_id
    FROM loanacceptedbybankinglicensepartnerbr_br
    UNION ALL
    SELECT 
        NULL as applicable_rate,application_id,approved_amount,client_id,NULL as current_installment_amount,NULL as custom_fincore_included,NULL as days_past_due,NULL as delinquency_balance,down_payment_amount,effective_annual_rate,NULL as first_payment_date,NULL as full_payment,NULL as guarantee_overdue,NULL as initial_installment_amount,NULL as interest_on_overdue_principal,NULL as interest_overdue,NULL as is_fully_paid,NULL as late_fees,loan_id,loan_type,NULL as min_payment,NULL as months_on_book,ocurred_on,origination_date,NULL as paid_installments,NULL as principal_overdue,NULL as state,surrogate_key,term,NULL as total_interest_paid,NULL as total_late_fees_condoned,NULL as total_late_fees_paid,NULL as total_payment_applied,NULL as total_principal_paid,NULL as unpaid_guarantee,NULL as unpaid_principal,
    event_name,
    event_id
    FROM loanacceptedwasnotsenttobankinglicensepartnerbr_br
    
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
this: silver.f_fincore_loans_br_logs
country: br
silver_table_name: f_fincore_loans_br_logs
table_pk_fields: ['surrogate_key']
table_pk_amount: 1
fields_direct: ['applicable_rate', 'application_id', 'approved_amount', 'client_id', 'current_installment_amount', 'custom_fincore_included', 'days_past_due', 'delinquency_balance', 'down_payment_amount', 'effective_annual_rate', 'event_id', 'first_payment_date', 'full_payment', 'guarantee_overdue', 'initial_installment_amount', 'interest_on_overdue_principal', 'interest_overdue', 'is_fully_paid', 'late_fees', 'loan_id', 'loan_type', 'min_payment', 'months_on_book', 'ocurred_on', 'origination_date', 'paid_installments', 'principal_overdue', 'state', 'surrogate_key', 'term', 'total_interest_paid', 'total_late_fees_condoned', 'total_late_fees_paid', 'total_payment_applied', 'total_principal_paid', 'unpaid_guarantee', 'unpaid_principal']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'clientloansstatusupdatedv2_unnested_by_loan_id': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['surrogate_key', 'event_id', 'loan_id', 'client_id', 'origination_date', 'term', 'days_past_due', 'paid_installments', 'effective_annual_rate', 'approved_amount', 'total_principal_paid', 'min_payment', 'full_payment', 'delinquency_balance', 'principal_overdue', 'interest_overdue', 'guarantee_overdue', 'unpaid_principal', 'interest_on_overdue_principal', 'initial_installment_amount', 'current_installment_amount', 'unpaid_guarantee', 'total_payment_applied', 'state', 'first_payment_date', 'months_on_book', 'applicable_rate', 'total_interest_paid', 'is_fully_paid', 'late_fees', 'total_late_fees_paid', 'total_late_fees_condoned', 'custom_fincore_included', 'ocurred_on'], 'custom_attributes': {}}, 'ProspectLoanAcceptedByBankingLicensePartner': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['surrogate_key', 'event_id', 'loan_id', 'client_id', 'application_id', 'origination_date', 'term', 'effective_annual_rate', 'approved_amount', 'ocurred_on'], 'custom_attributes': {}}, 'LoanAcceptedByBankingLicensePartnerBR': {'stage': 'loan_acceptance_br', 'direct_attributes': ['surrogate_key', 'event_id', 'loan_id', 'client_id', 'application_id', 'origination_date', 'term', 'loan_type', 'effective_annual_rate', 'approved_amount', 'down_payment_amount', 'ocurred_on'], 'custom_attributes': {}}, 'LoanAcceptedWasNotSentToBankingLicensePartnerBR': {'stage': 'loan_acceptance_br', 'direct_attributes': ['surrogate_key', 'event_id', 'loan_id', 'client_id', 'application_id', 'origination_date', 'term', 'loan_type', 'effective_annual_rate', 'approved_amount', 'down_payment_amount', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['clientloansstatusupdatedv2_unnested_by_loan_id', 'ProspectLoanAcceptedByBankingLicensePartner', 'LoanAcceptedByBankingLicensePartnerBR', 'LoanAcceptedWasNotSentToBankingLicensePartnerBR']
flag_group_feature_active: False
version: silver_sql_builder
*/
