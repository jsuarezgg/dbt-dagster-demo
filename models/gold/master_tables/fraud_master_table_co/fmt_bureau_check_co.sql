{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH prev_phones_emails AS (
    SELECT 
        a.application_id,
        orig.loan_id,
        a.client_id,
        a.application_date,
        a.application_cellphone,
        lower(a.application_email) AS application_email,
        coalesce(idv.ip_address, iov.iovation_data_details_realIp_address) as ip_address,
        sum(CASE WHEN a.application_cellphone != a2.application_cellphone AND a.application_cellphone IS NOT NULL AND a2.application_cellphone IS NOT NULL THEN 1 END) AS diff_past_cellphones,
        sum(CASE WHEN lower(a.application_email) != lower(a2.application_email) AND a.application_email IS NOT NULL AND a2.application_email IS NOT NULL THEN 1 END) AS diff_past_emails
     FROM {{ ref('f_pii_applications_co') }} a
     LEFT JOIN {{ ref('f_pii_applications_co') }} a2 
         ON a.client_id = a2.client_id
         AND a.application_date > a2.application_date
         AND (a.application_cellphone != a2.application_cellphone OR lower(a.application_email) != lower(a2.application_email))
     left join {{ ref('f_idv_stage_co') }} idv
        on a.application_id = idv.application_id
     left join {{ ref('f_kyc_iovation_v1v2_co') }} iov
        on a.application_id = iov.application_id
     LEFT JOIN {{ ref('f_originations_bnpl_co') }} orig
         ON a.application_id = orig.application_id
     GROUP BY 1,2,3,4,5,6,7
)
SELECT 
    a.*,
    b.id_exp_city,
    CASE
        WHEN instr(b.cellphones_string, a.application_cellphone) > 0 THEN 1 ELSE 0
    END AS cellphone_match,
    CASE
        WHEN instr(lower(b.emails_string), lower(a.application_email)) > 0 THEN 1 ELSE 0
    END AS email_match
FROM prev_phones_emails a
LEFT JOIN gold.fmt_kyc_all_info_co b 
    ON a.application_id = b.application_id
