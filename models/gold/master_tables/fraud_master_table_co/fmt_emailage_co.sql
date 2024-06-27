{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

select 
application_id,
emailAge_email_value as em_email,
emailAge_advice_value as em_advice,
emailAge_score as em_score,
emailAge_domain_creationDays as em_domain_creation_days,
emailAge_reason_value as em_reason,
emailAge_status_value as em_status,
emailAge_riskBand_value as em_risk_band,
emailAge_lastVerificationDate as em_last_verification_date,
emailAge_firstVerificationDate as em_first_verification_date
from {{ ref('f_kyc_emailage_co') }};
