{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
  loan_id,
  origination_date,
  m_vintage,
  amount_originated_this_month,
  moratory_interest_covered_this_month,
  interest_overdue_covered_this_month,
  principal_overdue_covered_this_month,
  guarantee_overdue_covered_this_month,
  current_interest_covered_this_month,
  current_principal_covered_this_month,
  current_guarantee_covered_this_month,
  unpaid_guarantee_covered_this_month,
  unpaid_principal_covered_this_month,
  prepayment_benefit_this_month,
  moratory_interest_condoned_this_month,
  interest_overdue_condoned_this_month,
  principal_overdue_condoned_this_month,
  guarantee_overdue_condoned_this_month,
  current_interest_condoned_this_month,
  current_principal_condoned_this_month,
  current_guarantee_condoned_this_month,
  unpaid_guarantee_condoned_this_month,
  unpaid_principal_condoned_this_month,
  interest_overdue_into_upb_this_month,
  interest_accrued_for_period_in_next_month,
  moratory_interest,
  interest_overdue,
  principal_overdue,
  guarantee_overdue,
  current_interest,
  current_principal,
  current_guarantee,
  unpaid_guarantee,
  unpaid_principal,
  unpaid_interest,
  total_current_interest_condoned,
  total_current_principal_condoned,
  total_interest_overdue_condoned,
  total_principal_overdue_condoned,
  total_unpaid_principal_condoned,
  interest_accrued_this_month,
  moratory_interest_accrued_this_month,
  first_period_interest_condonation_next_month,
  first_period_interest_condonation_this_month,
  client_total_payment,
  client_total_payment_addi,
  client_total_payment_pa,
  late_fees_covered_this_month AS collection_fees_covered_this_month,
  late_fees_condoned_this_month AS collection_fees_condoned_this_month,
  late_fees_accrued_this_month AS collection_fees_accrued_this_month,
  total_late_fees_condoned AS total_collection_fees_condoned,
  late_fees AS unpaid_collection_fees,
  client_id,
  prepayment_benefit_to_principal_this_month,
  prepayment_benefit_to_guarantee_this_month,
  bucket,
  days_past_due,
  created_at,
  total_fga_principal_recovered,
  total_fga_interest_recovered,
  fga_principal_recovered_this_month,
  fga_interest_recovered_this_month,
  total_late_fees_paid AS total_collection_fees_paid,
  late_fees_without_iva_accrued_this_month AS collection_fees_without_iva_accrued_this_month,
  late_fees_iva_accrued_this_month AS collection_fees_iva_accrued_this_month,
  ingested_at,
  updated_at
FROM {{ ref('trust_report_trust_accounting_report_co') }}
