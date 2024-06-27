

WITH applications_backfill_step_1 AS (
    SELECT
        application_id,
        MIN(fst.ocurred_on) AS min_ocurred_on,
        FIRST_VALUE(fst.ally_slug) AS ally_slug,
        FIRST_VALUE(fst.channel) AS channel,
        FIRST_VALUE(fst.client_id) AS client_id,
        FIRST_VALUE(fst.client_type) AS client_type,
        FIRST_VALUE(fst.journey_name) AS journey_name
    FROM silver.f_origination_events_co_logs fst
    
        WHERE from_utc_timestamp(ocurred_on, 'America/Bogota') > (SELECT MAX(ocurred_on_timestamp) FROM gold.dm_application_id_funnel_co)
    
    GROUP BY 1
),

applications_backfill_step_2 AS (
    SELECT
        fst.application_id,
        fst.min_ocurred_on,
        COALESCE(fst.journey_name, a.journey_name) AS journey_name,
        COALESCE(fst.ally_slug, a.ally_slug) AS ally_slug,
        COALESCE(fst.channel, a.channel) AS channel,
        COALESCE(fst.client_id, a.client_id) AS client_id,
        COALESCE(fst.client_type, a.client_type) AS client_type,
        COALESCE(synthetic.journey_name_synthetic, fst.journey_name) AS journey_name_synthetic
    FROM applications_backfill_step_1 fst
    LEFT JOIN (
        SELECT DISTINCT
            application_id,
            'PROSPECT_FINANCIA_SANTANDER_ADDI_CO' AS journey_name_synthetic
        FROM silver.f_origination_events_co_logs fs2
        WHERE journey_name = 'PROSPECT_FINANCIA_SANTANDER_CO'
            AND journey_stage_name IN ('underwriting-co')
            
                AND from_utc_timestamp(ocurred_on, 'America/Bogota') > (SELECT MAX(ocurred_on_timestamp) FROM gold.dm_application_id_funnel_co)
            
    ) AS synthetic ON fst.application_id = synthetic.application_id
    LEFT JOIN silver.f_applications_co AS a ON fst.application_id = a.application_id
),

application_preapproval_co_filtered AS (
    SELECT
        a.client_id,
        a.application_date,
        a.preapproval_expiration_date,
        a.preapproval_amount
    FROM silver.f_applications_co AS a
    INNER JOIN (
        SELECT
            application_id,
            row_number() over(partition by client_id, application_date::date order by application_date) as rn
        FROM silver.f_applications_co
        WHERE channel = 'PRE_APPROVAL'
          AND custom_is_preapproval_completed IS TRUE
    ) AS filtered_apps ON a.application_id = filtered_apps.application_id
    WHERE filtered_apps.rn = 1
),

preapproval_flagged_applications AS (
    SELECT DISTINCT
        a.application_id,
        1 AS flag_preapproval
    FROM silver.f_applications_co AS a
    INNER JOIN application_preapproval_co_filtered AS pf
        ON a.client_id = pf.client_id AND a.application_date > pf.application_date AND a.application_date <= pf.preapproval_expiration_date
    LEFT JOIN applications_backfill_step_2 AS ab ON a.application_id = ab.application_id
    WHERE a.requested_amount <= pf.preapproval_amount
        AND ab.client_type = 'PROSPECT'
        AND a.journey_name NOT ilike '%preap%'
),

terminated_applications AS (
    SELECT DISTINCT
        application_id
    FROM silver.f_origination_events_co_logs
    WHERE event_type in ('DECLINATION','ABANDONMENT','REJECTION','APPROVAL')
),

vertical_brand AS (
    SELECT
        ally_name as ally_slug,
        UPPER(get_json_object(ally_vertical,'$.name.value')) AS vertical,
        UPPER(get_json_object(ally_brand,'$.name.value')) AS brand
    FROM silver.d_ally_management_stores_allies_co
),

privacy_policy_screens AS (
    SELECT
        application_id,
        1 AS privacy_policy_stage_in,
        MAX(CASE WHEN event_name = 'ClientFirstLastNameConfirmedCO' THEN 1 ELSE 0 END) AS privacy_first_name_co_out,
        MAX(CASE WHEN event_name = 'ClientNationalIdentificationExpeditionDateConfirmedCO' THEN 1 ELSE 0 END) AS privacy_expiration_date_co_out,
        MAX(CASE WHEN event_name = 'PrivacyPolicyAcceptedCO' THEN 1 ELSE 0 END) AS privacy_accepted_co_out
        FROM silver.f_origination_events_co_logs
        WHERE journey_stage_name IN ('privacy-policy-co') AND ocurred_on > '2022-07-29 19:50:00'
            
                AND from_utc_timestamp(ocurred_on, 'America/Bogota') > (SELECT MAX(ocurred_on_timestamp) FROM gold.dm_application_id_funnel_co)
            
    GROUP BY 1
),

funnel_step_1 AS (
    SELECT
        fst.application_id,
        COALESCE(ab.ally_slug, fst.ally_slug) AS ally_slug,
        COALESCE(ab.client_id, fst.client_id) AS client_id,
        COALESCE(ab.journey_name_synthetic, fst.journey_name) AS journey_name,
        fst.journey_stage_name,
        CASE
            WHEN COALESCE(ab.client_type, fst.client_type) = 'LEAD' THEN 'PROSPECT'
            ELSE COALESCE(ab.client_type, fst.client_type)
        END AS client_type,
        COALESCE(ab.channel, fst.channel) AS channel,
        pfa.flag_preapproval,
        vb.brand AS brand,
        vb.vertical AS vertical,
        pps.privacy_policy_stage_in,
        pps.privacy_first_name_co_out,
        pps.privacy_expiration_date_co_out,
        pps.privacy_accepted_co_out,
        MAX(CASE WHEN event_type IN ('APPROVAL') THEN 1 ELSE 0 END) OVER (PARTITION BY fst.application_id) AS flag_approval_event,
        CASE
            WHEN fst.event_type IN ('DECLINATION','ABANDONMENT','REJECTION') THEN 0
            ELSE 1
        END AS flag_out,
        MIN(from_utc_timestamp(fst.ocurred_on,'America/Bogota')) OVER (PARTITION BY fst.application_id) AS ocurred_on_creation,
        MAX(CASE WHEN fst.event_type = 'APPROVAL' THEN from_utc_timestamp(fst.ocurred_on,'America/Bogota') END) OVER (PARTITION BY fst.application_id) AS ocurred_on_approval
    FROM silver.f_origination_events_co_logs AS fst
    INNER JOIN terminated_applications AS ta     			ON fst.application_id = ta.application_id
    LEFT JOIN vertical_brand AS vb               			ON fst.ally_slug = vb.ally_slug
    LEFT JOIN preapproval_flagged_applications AS pfa       ON fst.application_id = pfa.application_id
    LEFT JOIN privacy_policy_screens AS pps                 ON fst.application_id = pps.application_id
    LEFT JOIN applications_backfill_step_2 AS ab            ON fst.application_id = ab.application_id
    
        WHERE from_utc_timestamp(fst.ocurred_on, 'America/Bogota') > (SELECT MAX(ocurred_on_timestamp) FROM gold.dm_application_id_funnel_co)
    
),

funnel_step_2 AS (
    SELECT
        application_id,
        client_id,
        client_type,
        channel,
        ally_slug,
        brand,
        vertical,
        journey_name,
        journey_stage_name,
        flag_preapproval,
        privacy_policy_stage_in,
        privacy_first_name_co_out,
        privacy_expiration_date_co_out,
        privacy_accepted_co_out,
        MAX(flag_approval_event) AS flag_approval_event,
        GREATEST(MIN(flag_out),MAX(flag_approval_event)) AS flag_out,
        MAX(COALESCE(ocurred_on_approval,ocurred_on_creation)) AS ocurred_on_timestamp,
        MAX(ocurred_on_approval) AS ocurred_on_approval,
        MIN(ocurred_on_creation) AS ocurred_on_creation
    FROM funnel_step_1
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
),

funnel_step_3 AS (
    SELECT
        application_id,
        client_id,
        ally_slug,
        journey_name,
        journey_stage_name,
        client_type,
        brand,
        vertical,
        channel,
        TO_DATE(ocurred_on_timestamp) AS ocurred_on_date,
        CASE
            WHEN journey_name IN ('PROSPECT_CHECKPOINT_FINANCIA_CO', 'PROSPECT_FINANCIA_CO', 'PROSPECT_FINANCIA_SANTANDER_ADDI_CO', 'PROSPECT_GATEWAY_FINANCIA_CO') THEN 'CO-FINANCIA'
            WHEN journey_name IN ('PREAPPROVAL_PAGO_CO','PREAPPROVAL_CHECKPOINT_PAGO_CO') THEN 'CO-PREAPPROVAL'
            WHEN journey_name IN ('PROSPECT_FINANCIA_SANTANDER_CO') THEN 'CO-SANTANDER'
            WHEN journey_name IN ('PROSPECT_PAGO_CO','PROSPECT_CHECKPOINT_PAGO_CO','CLIENT_PAGO_CO','CLIENT_FINANCIA_CO','PROSPECT_GATEWAY_PAGO_CO') THEN 'CO-PAGO'
        END AS journey_homolog,
        CASE
            WHEN journey_stage_name IN ('additional-information-santander-co') THEN 'additional-information-co'
            WHEN journey_stage_name IN ('background-check-co') THEN 'background-check-co'
            WHEN journey_stage_name IN ('basic-identity-co') THEN 'basic-identity-co'
            WHEN journey_stage_name IN ('device-information') THEN 'device-information-co'
            WHEN journey_stage_name IN ('fraud-check-co','fraud-check-rc-co') THEN 'fraud-check-co'
            WHEN journey_stage_name IN ('identity-photos','identity-wa') THEN 'identity-verification-co'
            WHEN journey_stage_name IN ('loan-acceptance-co','loan-acceptance-santander-co') THEN 'loan-acceptance-co'
            WHEN journey_stage_name IN ('loan-proposals-co','loan-proposals-santander-co') THEN 'loan-proposals-co'
            WHEN journey_stage_name IN ('preapproval-summary-co') THEN 'preapproval-summary-co'
            WHEN journey_stage_name IN ('preconditions-co','preconditions-pago-co') THEN 'preconditions-co'
            WHEN journey_stage_name IN ('privacy-policy-co','privacy-policy-santander-co') THEN 'privacy-policy-co'
            WHEN journey_stage_name IN ('psychometric_assessment') THEN 'psychometric-assessment-co'
            WHEN journey_stage_name IN ('risk-evaluation-santander-co','underwriting-co','underwriting-pago-co','underwriting-preapproval-co','underwriting-preapproval-pago-co','underwriting-psychometric-co','underwriting-rc-co','underwriting-rc-pago-co') THEN 'underwriting-co'
            WHEN journey_stage_name IN ('work-information-santander-co') THEN 'work-information-co'
        END AS journey_stage_homolog,
        MAX(ocurred_on_approval) AS ocurred_on_approval,
        MIN(ocurred_on_creation) AS ocurred_on_creation,
        MAX(flag_approval_event) AS flag_approval_event,
        MAX(flag_out) AS flag_out,
        MAX(flag_preapproval) AS flag_preapproval,
        MAX(privacy_policy_stage_in) AS privacy_policy_stage_in,
        MAX(privacy_first_name_co_out) AS privacy_first_name_co_out,
        MAX(privacy_expiration_date_co_out) AS privacy_expiration_date_co_out,
        MAX(privacy_accepted_co_out) AS privacy_accepted_co_out,
        1 AS flag_in
    FROM funnel_step_2
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

funnel_step_4 AS (
    SELECT
        application_id,
        client_id,
        ally_slug,
        journey_name,
        journey_homolog,
        client_type,
        brand,
        vertical,
        ocurred_on_date,
        channel,
        md5(CONCAT(ocurred_on_date, client_id, ally_slug, journey_name)) AS application_process_id,
        COALESCE(MAX(ocurred_on_approval), MIN(ocurred_on_creation)) AS ocurred_on_timestamp,
        MAX(CASE WHEN journey_stage_homolog IN ('additional-information-co') THEN flag_in ELSE 0 END) AS additional_information_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('additional-information-co') THEN flag_out ELSE 0 END) AS additional_information_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('background-check-co') THEN flag_in ELSE 0 END) AS background_check_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('background-check-co') THEN flag_out ELSE 0 END) AS background_check_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('basic-identity-co') THEN flag_in ELSE 0 END) AS basic_identity_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('basic-identity-co') THEN flag_out ELSE 0 END) AS basic_identity_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('device-information-co') THEN flag_in ELSE 0 END) AS device_information_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('device-information-co') THEN flag_out ELSE 0 END) AS device_information_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('fraud-check-co') THEN flag_in ELSE 0 END) AS fraud_check_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('fraud-check-co') THEN flag_out ELSE 0 END) AS fraud_check_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('identity-verification-co') THEN flag_in ELSE 0 END) AS identity_verification_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('identity-verification-co') THEN flag_out ELSE 0 END) AS identity_verification_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('loan-acceptance-co') THEN flag_in ELSE 0 END) AS loan_acceptance_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('loan-acceptance-co') THEN flag_out ELSE 0 END) AS loan_acceptance_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('loan-proposals-co') THEN flag_in ELSE 0 END) AS loan_proposals_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('loan-proposals-co') THEN flag_out ELSE 0 END) AS loan_proposals_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('preapproval-summary-co') THEN flag_in ELSE 0 END) AS preapproval_summary_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('preapproval-summary-co') THEN flag_out ELSE 0 END) AS preapproval_summary_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('preconditions-co') THEN flag_in ELSE 0 END) AS preconditions_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('preconditions-co') THEN flag_out ELSE 0 END) AS preconditions_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('privacy-policy-co') THEN flag_in ELSE 0 END) AS privacy_policy_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('privacy-policy-co') THEN flag_out ELSE 0 END) AS privacy_policy_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('psychometric-assessment-co') THEN flag_in ELSE 0 END) AS psychometric_assessment_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('psychometric-assessment-co') THEN flag_out ELSE 0 END) AS psychometric_assessment_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('underwriting-co') THEN flag_in ELSE 0 END) AS underwriting_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('underwriting-co') THEN flag_out ELSE 0 END) AS underwriting_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('work-information-co') THEN flag_in ELSE 0 END) AS work_information_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('work-information-co') THEN flag_out ELSE 0 END) AS work_information_co_out,
        MAX(flag_approval_event) AS flag_approval_event,
        MAX(flag_preapproval) AS flag_preapproval,
        MAX(privacy_policy_stage_in) AS privacy_policy_stage_in,
        MAX(privacy_first_name_co_out) AS privacy_first_name_co_out,
        MAX(privacy_accepted_co_out) AS privacy_accepted_co_out,
        MAX(privacy_expiration_date_co_out) AS privacy_expiration_date_co_out
    FROM funnel_step_3
    WHERE journey_homolog IS NOT NULL
    GROUP BY 1,2,3,4,5,6,7,8,9,10
)

SELECT
    application_id,
    application_process_id,
    ocurred_on_date,
    ocurred_on_timestamp,
    client_id,
    journey_name,
    journey_homolog,
    ally_slug,
    brand,
    vertical,
    channel,
    client_type,
    COALESCE(flag_preapproval, 0) AS flag_preapproval,
    flag_approval_event,
    additional_information_co_in,
    additional_information_co_out,
    background_check_co_in,
    background_check_co_out,
    basic_identity_co_in,
    basic_identity_co_out,
    device_information_co_in,
    device_information_co_out,
    fraud_check_co_in,
    fraud_check_co_out,
    identity_verification_co_in,
    identity_verification_co_out,
    loan_acceptance_co_in,
    loan_acceptance_co_out,
    loan_proposals_co_in,
    loan_proposals_co_out,
    preapproval_summary_co_in,
    preapproval_summary_co_out,
    preconditions_co_in,
    preconditions_co_out,
    privacy_policy_stage_in,
    privacy_accepted_co_out,
    privacy_expiration_date_co_out,
    privacy_first_name_co_out,
    privacy_policy_co_in,
    privacy_policy_co_out,
    psychometric_assessment_co_in,
    psychometric_assessment_co_out,
    underwriting_co_in,
    underwriting_co_out,
    work_information_co_in,
    work_information_co_out
FROM funnel_step_4 AS fs4