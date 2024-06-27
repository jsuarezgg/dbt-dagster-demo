
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
        applicable_rate,application_id,approved_amount,client_id,current_installment_amount,custom_fincore_included,days_past_due,delinquency_balance,down_payment_amount,effective_annual_rate,first_payment_date,full_payment,guarantee_overdue,initial_installment_amount,interest_on_overdue_principal,interest_overdue,is_fully_paid,late_fees,loan_id,loan_type,min_payment,months_on_book,last_event_ocurred_on_processed as ocurred_on,origination_date,paid_installments,principal_overdue,state,term,total_interest_paid,total_late_fees_condoned,total_late_fees_paid,total_payment_applied,total_principal_paid,unpaid_guarantee,unpaid_principal,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_fincore_loans_br
    WHERE silver.f_fincore_loans_br.ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30')
    UNION ALL
    SELECT 
        applicable_rate,NULL as application_id,approved_amount,client_id,current_installment_amount,custom_fincore_included,days_past_due,delinquency_balance,NULL as down_payment_amount,effective_annual_rate,first_payment_date,full_payment,guarantee_overdue,initial_installment_amount,interest_on_overdue_principal,interest_overdue,is_fully_paid,late_fees,loan_id,NULL as loan_type,min_payment,months_on_book,ocurred_on,origination_date,paid_installments,principal_overdue,state,term,total_interest_paid,total_late_fees_condoned,total_late_fees_paid,total_payment_applied,total_principal_paid,unpaid_guarantee,unpaid_principal,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientloansstatusupdatedv2_unnested_by_loan_id_br
    UNION ALL
    SELECT 
        NULL as applicable_rate,application_id,approved_amount,client_id,NULL as current_installment_amount,NULL as custom_fincore_included,NULL as days_past_due,NULL as delinquency_balance,NULL as down_payment_amount,effective_annual_rate,NULL as first_payment_date,NULL as full_payment,NULL as guarantee_overdue,NULL as initial_installment_amount,NULL as interest_on_overdue_principal,NULL as interest_overdue,NULL as is_fully_paid,NULL as late_fees,loan_id,NULL as loan_type,NULL as min_payment,NULL as months_on_book,ocurred_on,origination_date,NULL as paid_installments,NULL as principal_overdue,NULL as state,term,NULL as total_interest_paid,NULL as total_late_fees_condoned,NULL as total_late_fees_paid,NULL as total_payment_applied,NULL as total_principal_paid,NULL as unpaid_guarantee,NULL as unpaid_principal,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectloanacceptedbybankinglicensepartner_br
    UNION ALL
    SELECT 
        NULL as applicable_rate,application_id,approved_amount,client_id,NULL as current_installment_amount,NULL as custom_fincore_included,NULL as days_past_due,NULL as delinquency_balance,down_payment_amount,effective_annual_rate,NULL as first_payment_date,NULL as full_payment,NULL as guarantee_overdue,NULL as initial_installment_amount,NULL as interest_on_overdue_principal,NULL as interest_overdue,NULL as is_fully_paid,NULL as late_fees,loan_id,loan_type,NULL as min_payment,NULL as months_on_book,ocurred_on,origination_date,NULL as paid_installments,NULL as principal_overdue,NULL as state,term,NULL as total_interest_paid,NULL as total_late_fees_condoned,NULL as total_late_fees_paid,NULL as total_payment_applied,NULL as total_principal_paid,NULL as unpaid_guarantee,NULL as unpaid_principal,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanacceptedbybankinglicensepartnerbr_br
    UNION ALL
    SELECT 
        NULL as applicable_rate,application_id,approved_amount,client_id,NULL as current_installment_amount,NULL as custom_fincore_included,NULL as days_past_due,NULL as delinquency_balance,down_payment_amount,effective_annual_rate,NULL as first_payment_date,NULL as full_payment,NULL as guarantee_overdue,NULL as initial_installment_amount,NULL as interest_on_overdue_principal,NULL as interest_overdue,NULL as is_fully_paid,NULL as late_fees,loan_id,loan_type,NULL as min_payment,NULL as months_on_book,ocurred_on,origination_date,NULL as paid_installments,NULL as principal_overdue,NULL as state,term,NULL as total_interest_paid,NULL as total_late_fees_condoned,NULL as total_late_fees_paid,NULL as total_payment_applied,NULL as total_principal_paid,NULL as unpaid_guarantee,NULL as unpaid_principal,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanacceptedwasnotsenttobankinglicensepartnerbr_br
    
)


-- SECTION 3 -> merge events by key and keep last not null data in each field
, grouped_events AS (
  select
    loan_id,
    element_at(array_sort(array_agg(CASE WHEN applicable_rate is not null then struct(ocurred_on, applicable_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).applicable_rate as applicable_rate,
    element_at(array_sort(array_agg(CASE WHEN application_id is not null then struct(ocurred_on, application_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_id as application_id,
    element_at(array_sort(array_agg(CASE WHEN approved_amount is not null then struct(ocurred_on, approved_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).approved_amount as approved_amount,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN current_installment_amount is not null then struct(ocurred_on, current_installment_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).current_installment_amount as current_installment_amount,
    element_at(array_sort(array_agg(CASE WHEN custom_fincore_included is not null then struct(ocurred_on, custom_fincore_included) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_fincore_included as custom_fincore_included,
    element_at(array_sort(array_agg(CASE WHEN days_past_due is not null then struct(ocurred_on, days_past_due) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).days_past_due as days_past_due,
    element_at(array_sort(array_agg(CASE WHEN delinquency_balance is not null then struct(ocurred_on, delinquency_balance) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).delinquency_balance as delinquency_balance,
    element_at(array_sort(array_agg(CASE WHEN down_payment_amount is not null then struct(ocurred_on, down_payment_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).down_payment_amount as down_payment_amount,
    element_at(array_sort(array_agg(CASE WHEN effective_annual_rate is not null then struct(ocurred_on, effective_annual_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).effective_annual_rate as effective_annual_rate,
    element_at(array_sort(array_agg(CASE WHEN first_payment_date is not null then struct(ocurred_on, first_payment_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_payment_date as first_payment_date,
    element_at(array_sort(array_agg(CASE WHEN full_payment is not null then struct(ocurred_on, full_payment) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).full_payment as full_payment,
    element_at(array_sort(array_agg(CASE WHEN guarantee_overdue is not null then struct(ocurred_on, guarantee_overdue) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).guarantee_overdue as guarantee_overdue,
    element_at(array_sort(array_agg(CASE WHEN initial_installment_amount is not null then struct(ocurred_on, initial_installment_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).initial_installment_amount as initial_installment_amount,
    element_at(array_sort(array_agg(CASE WHEN interest_on_overdue_principal is not null then struct(ocurred_on, interest_on_overdue_principal) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).interest_on_overdue_principal as interest_on_overdue_principal,
    element_at(array_sort(array_agg(CASE WHEN interest_overdue is not null then struct(ocurred_on, interest_overdue) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).interest_overdue as interest_overdue,
    element_at(array_sort(array_agg(CASE WHEN is_fully_paid is not null then struct(ocurred_on, is_fully_paid) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).is_fully_paid as is_fully_paid,
    element_at(array_sort(array_agg(CASE WHEN late_fees is not null then struct(ocurred_on, late_fees) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).late_fees as late_fees,
    element_at(array_sort(array_agg(CASE WHEN loan_type is not null then struct(ocurred_on, loan_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_type as loan_type,
    element_at(array_sort(array_agg(CASE WHEN min_payment is not null then struct(ocurred_on, min_payment) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).min_payment as min_payment,
    element_at(array_sort(array_agg(CASE WHEN months_on_book is not null then struct(ocurred_on, months_on_book) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).months_on_book as months_on_book,
    element_at(array_sort(array_agg(CASE WHEN origination_date is not null then struct(ocurred_on, origination_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).origination_date as origination_date,
    element_at(array_sort(array_agg(CASE WHEN paid_installments is not null then struct(ocurred_on, paid_installments) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).paid_installments as paid_installments,
    element_at(array_sort(array_agg(CASE WHEN principal_overdue is not null then struct(ocurred_on, principal_overdue) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).principal_overdue as principal_overdue,
    element_at(array_sort(array_agg(CASE WHEN state is not null then struct(ocurred_on, state) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).state as state,
    element_at(array_sort(array_agg(CASE WHEN term is not null then struct(ocurred_on, term) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).term as term,
    element_at(array_sort(array_agg(CASE WHEN total_interest_paid is not null then struct(ocurred_on, total_interest_paid) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).total_interest_paid as total_interest_paid,
    element_at(array_sort(array_agg(CASE WHEN total_late_fees_condoned is not null then struct(ocurred_on, total_late_fees_condoned) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).total_late_fees_condoned as total_late_fees_condoned,
    element_at(array_sort(array_agg(CASE WHEN total_late_fees_paid is not null then struct(ocurred_on, total_late_fees_paid) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).total_late_fees_paid as total_late_fees_paid,
    element_at(array_sort(array_agg(CASE WHEN total_payment_applied is not null then struct(ocurred_on, total_payment_applied) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).total_payment_applied as total_payment_applied,
    element_at(array_sort(array_agg(CASE WHEN total_principal_paid is not null then struct(ocurred_on, total_principal_paid) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).total_principal_paid as total_principal_paid,
    element_at(array_sort(array_agg(CASE WHEN unpaid_guarantee is not null then struct(ocurred_on, unpaid_guarantee) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).unpaid_guarantee as unpaid_guarantee,
    element_at(array_sort(array_agg(CASE WHEN unpaid_principal is not null then struct(ocurred_on, unpaid_principal) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).unpaid_principal as unpaid_principal,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 1
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
this: silver.f_fincore_loans_br
country: br
silver_table_name: f_fincore_loans_br
table_pk_fields: ['loan_id']
table_pk_amount: 1
fields_direct: ['applicable_rate', 'application_id', 'approved_amount', 'client_id', 'current_installment_amount', 'custom_fincore_included', 'days_past_due', 'delinquency_balance', 'down_payment_amount', 'effective_annual_rate', 'first_payment_date', 'full_payment', 'guarantee_overdue', 'initial_installment_amount', 'interest_on_overdue_principal', 'interest_overdue', 'is_fully_paid', 'late_fees', 'loan_id', 'loan_type', 'min_payment', 'months_on_book', 'ocurred_on', 'origination_date', 'paid_installments', 'principal_overdue', 'state', 'term', 'total_interest_paid', 'total_late_fees_condoned', 'total_late_fees_paid', 'total_payment_applied', 'total_principal_paid', 'unpaid_guarantee', 'unpaid_principal']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'clientloansstatusupdatedv2_unnested_by_loan_id': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_id', 'client_id', 'origination_date', 'term', 'days_past_due', 'paid_installments', 'effective_annual_rate', 'approved_amount', 'total_principal_paid', 'min_payment', 'full_payment', 'delinquency_balance', 'principal_overdue', 'interest_overdue', 'guarantee_overdue', 'unpaid_principal', 'interest_on_overdue_principal', 'initial_installment_amount', 'current_installment_amount', 'unpaid_guarantee', 'total_payment_applied', 'state', 'first_payment_date', 'months_on_book', 'applicable_rate', 'total_interest_paid', 'is_fully_paid', 'late_fees', 'total_late_fees_paid', 'total_late_fees_condoned', 'custom_fincore_included', 'ocurred_on'], 'custom_attributes': {}}, 'ProspectLoanAcceptedByBankingLicensePartner': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_id', 'client_id', 'application_id', 'origination_date', 'term', 'effective_annual_rate', 'approved_amount', 'ocurred_on'], 'custom_attributes': {}}, 'LoanAcceptedByBankingLicensePartnerBR': {'stage': 'loan_acceptance_br', 'direct_attributes': ['loan_id', 'client_id', 'application_id', 'origination_date', 'term', 'loan_type', 'effective_annual_rate', 'approved_amount', 'down_payment_amount', 'ocurred_on'], 'custom_attributes': {}}, 'LoanAcceptedWasNotSentToBankingLicensePartnerBR': {'stage': 'loan_acceptance_br', 'direct_attributes': ['loan_id', 'client_id', 'application_id', 'origination_date', 'term', 'loan_type', 'effective_annual_rate', 'approved_amount', 'down_payment_amount', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['clientloansstatusupdatedv2_unnested_by_loan_id', 'ProspectLoanAcceptedByBankingLicensePartner', 'LoanAcceptedByBankingLicensePartnerBR', 'LoanAcceptedWasNotSentToBankingLicensePartnerBR']
flag_group_feature_active: True
version: silver_sql_builder
*/
