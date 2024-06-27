

--raw.snc_client_loan_calculations_br
SELECT
    -- DIRECT MODELING FIELDS
	accrued_interest_this_month,
	applicable_rate,
	approved_amount,
	calculation_date,
	cancellation_reason,
	client_delinquency_balance,
	client_full_payment,
	client_id,
	client_min_payment,
	cast(client_payday as date),
	client_total_payment,
	client_total_payment_addi,
	client_total_payment_pa,
	condoned_interest_this_month,
	current_guarantee_due_this_period,
	current_installment_amount,
	current_interest_covered_this_month,
	current_interest_covered_this_period,
	current_interest_due_this_period,
	current_principal_covered_this_month,
	current_principal_covered_this_period,
	current_principal_due_this_period,
	days_past_due,
	delinquency_iof,
	effective_annual_rate,
	first_payment_date,
	full_payment,
	guarantee_covered_this_month,
	guarantee_covered_this_period,
	guarantee_overdue,
	initial_installment_amount,
	initial_iof_amount,
	interest_condoned_in_first_period,
	interest_on_overdue_principal,
	interest_on_overdue_principal_covered_this_month,
	interest_on_overdue_principal_covered_this_period,
	interest_overdue,
	interest_overdue_covered_this_month,
	interest_overdue_covered_this_period,
	iof_additional_rate,
	iof_daily_rate,
	iof_installment_limit_rate,
	ipmt,
	is_cancelled,
	is_fully_paid,
	is_fully_paid_in_old_loan_tape,
	loan_id,
	loan_ownership,
	low_balance_loan,
	min_payment,
	months_on_books,
	origination_date,
	paid_installments,
	cast (payday as date),
	period_prepayment_benefit,
	positive_balance,
	ppmt,
	prepayment_benefit_this_month,
	prepayment_this_period,
	principal_overdue,
	principal_overdue_covered_this_month,
	principal_overdue_covered_this_period,
	term,
	total_applied_payment,
	total_current_interest_condoned,
	total_current_principal_condoned,
	total_delinquency_iof_paid,
	total_guarantee_condoned,
	total_guarantee_paid,
	total_interest_overdue_condoned,
	total_interest_paid,
	total_moratory_interest_condoned,
	total_prepayment_benefit,
	total_principal_overdue_condoned,
	total_principal_paid,
	total_unpaid_principal_condoned,
	unpaid_guarantee,
	unpaid_interest,
	unpaid_principal,
	unpaid_principal_covered_this_month,
	vintage,
	late_fees,
	total_late_fees_paid,
	total_late_fees_condoned,
	created_at,
	current_positive_balance,
	is_full_payment,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from raw.snc_client_loan_calculations_br