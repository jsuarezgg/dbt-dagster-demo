{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

select
    ocurred_on_date as day,
    client_id,
    ally_slug as ally_name,
    journey_name,
    max(background_check_br_in) as background_check_br_in,
    max(background_check_br_out) as background_check_br_out,
    max(device_information_br_in) as device_information_in,
    max(device_information_br_out) as device_information_out,
    max(fraud_check_br_in) as fraud_check_br_in,
    max(fraud_check_br_out) as fraud_check_br_out,
    --would remove:
    --max(fraud_check_rc_br_in) as fraud_check_rc_br_in,
    --max(fraud_check_rc_br_out) as fraud_check_rc_br_out,
    max(loan_proposals_br_in) as loan_proposals_br_in,
    max(loan_proposals_br_out) as loan_proposals_br_out,
    max(preconditions_br_in) as preconditions_in,
    max(preconditions_br_out) as preconditions_out,
    max(underwriting_br_in) as underwriting_in,
    max(underwriting_br_out) as underwriting_out,
    max(loan_acceptance_br_in) as loan_acceptance_in,
    max(loan_acceptance_br_out) as loan_acceptance_out,
    greatest(max(identity_verification_br_in), max(idv_third_party_br_in)) as identity_in,
    greatest(max(identity_verification_br_out), max(idv_third_party_br_out)) as identity_out,
    max(additional_information_br_in) as additional_information_br_in,
    max(additional_information_br_out) as additional_information_br_out,
    max(banking_license_partner_br_in) as banking_license_partner_br_in,
    max(banking_license_partner_br_out) as banking_license_partner_br_out,
    max(basic_identity_br_in) as basic_identity_br_in,
    max(basic_identity_br_out) as basic_identity_br_out,
    max(bn_pn_payments_br_in) as bn_pn_payments_br_in,
    max(bn_pn_payments_br_out) as bn_pn_payments_br_out,
    max(cellphone_validation_br_in) as cellphone_validation_br_in,
    max(cellphone_validation_br_out) as cellphone_validation_br_out,
    max(down_payment_br_in) as down_payment_br_in,
    max(down_payment_br_out) as down_payment_br_out,
    max(personal_information_br_in) as personal_information_br_in,
    max(personal_information_br_out) as personal_information_br_out,
    max(preapproval_summary_br_in) as preapproval_summary_br_in,
    max(preapproval_summary_br_out) as preapproval_summary_br_out,
    max(privacy_policy_br_in) as privacy_policy_br_in,
    max(privacy_policy_br_out) as privacy_policy_br_out,
    max(psychometric_assessment_br_in) as psychometric_assessment_br_in,
    max(psychometric_assessment_br_out) as psychometric_assessment_br_out,
    max(subproduct_selection_br_in) as subproduct_selection_br_in,
    max(subproduct_selection_br_out) as subproduct_selection_br_out
from {{ ref('dm_application_process_funnel_br') }}
group by 1, 2, 3, 4
