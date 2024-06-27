


WITH applications_backfill_step_1 AS (
	SELECT
	fst.application_id,
	MIN(fst.ocurred_on) AS min_ocurred_on,
    FIRST_VALUE(fst.ally_slug) AS ally_slug,
    FIRST_VALUE(fst.channel) AS channel,
    FIRST_VALUE(fst.client_id) AS client_id,
    FIRST_VALUE(fst.client_type) AS client_type,
    FIRST_VALUE(fst.journey_name) AS journey_name
    FROM silver.f_origination_events_br_logs fst
    GROUP BY 1
)
,
applications_backfill_step_2 AS (
	Select
	fst.application_id,
	fst.min_ocurred_on,
	COALESCE(fst.journey_name, a.journey_name) AS journey_name,
	COALESCE(fst.ally_slug, a.ally_slug) AS ally_slug,
	COALESCE(fst.channel, a.channel) AS channel,
	COALESCE(fst.client_id, a.client_id) AS client_id,
	COALESCE(fst.client_type, a.client_type, a.client_type) AS client_type,
	COALESCE(synthetic.journey_name_synthetic, fst.journey_name) AS journey_name_synthetic
	FROM applications_backfill_step_1 fst
	LEFT JOIN (
		SELECT
	            DISTINCT application_id, 'PROSPECT_PAGO_BNPN_BR_LEGACY' AS journey_name_synthetic
	        FROM silver.f_origination_events_br_logs fs2
	        WHERE journey_name IN ('CLIENT_PAGO_BR', 'PROSPECT_CHECKPOINT_PAGO_BR', 'PROSPECT_PAGO_BR', 'PROSPECT_PAGO_V2_BR')
	        AND journey_stage_name IN ('bn-pn-payments-br')
	) AS synthetic 		ON fst.application_id = synthetic.application_id
	LEFT JOIN silver.f_applications_br AS a 		ON fst.application_id = a.application_id
)
,
application_preapproval_br_filtered AS (
	SELECT
		a.client_id,
	    a.application_date,
	    a.preapproval_expiration_date,
	  	a.preapproval_amount
	FROM silver.f_applications_br AS a
	INNER JOIN (
		SELECT
			application_id,
			row_number() over(partition by client_id, application_date::date order by application_date) as rn
		FROM silver.f_applications_br
		WHERE channel = 'PRE_APPROVAL'
			AND custom_is_preapproval_completed IS TRUE
		) AS filtered_apps     ON a.application_id = filtered_apps.application_id
	WHERE filtered_apps.rn = 1
)
,
preapproval_flagged_applications AS (
	SELECT
		DISTINCT a.application_id,
		1 AS flag_preapproval
	FROM silver.f_applications_br AS a
	INNER JOIN application_preapproval_br_filtered AS pf  	ON a.client_id = pf.client_id
														AND a.application_date > pf.application_date and a.application_date <= pf.preapproval_expiration_date
	LEFT JOIN applications_backfill_step_2 AS ab            ON a.application_id = ab.application_id
	WHERE a.requested_amount <= pf.preapproval_amount
        AND ab.client_type = 'PROSPECT'
        AND a.journey_name NOT ILIKE '%preap%'
)
,
terminated_applications AS (
	SELECT
		DISTINCT application_id
	FROM silver.f_origination_events_br_logs
	WHERE event_type in ('DECLINATION','ABANDONMENT','REJECTION','APPROVAL')
)
,
vertical_brand AS (
  SELECT
      DISTINCT ally_slug,
      upper(get_json_object(ally_vertical,'$.name.value')) AS vertical
      ,upper(get_json_object(ally_brand,'$.name.value')) AS brand
  FROM silver.d_ally_management_stores_allies_br
)
,
funnel_step_1 AS (
  SELECT
	  fst.application_id,
	  COALESCE(ab.ally_slug, fst.ally_slug) AS ally_slug,
	  COALESCE(ab.client_id, fst.client_id) AS client_id,
	  COALESCE(ab.journey_name_synthetic, fst.journey_name) AS journey_name,
      vb.brand AS brand,
      vb.vertical AS vertical,
	  fst.journey_stage_name,
	  ab.client_type,
	  COALESCE(ab.channel, fst.channel) AS channel,
	  pfa.flag_preapproval,
      MAX(CASE WHEN event_type IN ('APPROVAL') THEN 1 ELSE 0 END) OVER (PARTITION BY fst.application_id) AS flag_approval_event,
	  CASE WHEN fst.event_type IN ('DECLINATION','ABANDONMENT','REJECTION') THEN 0 ELSE 1 END AS flag_out,
	  MIN(from_utc_timestamp(fst.ocurred_on,'America/Sao_Paulo')) OVER (PARTITION BY fst.application_id) AS ocurred_on_creation,
	  MAX(CASE WHEN fst.event_type = 'APPROVAL' THEN from_utc_timestamp(fst.ocurred_on,'America/Sao_Paulo') END) OVER (PARTITION BY fst.application_id) AS ocurred_on_approval
  FROM silver.f_origination_events_br_logs AS fst
  INNER JOIN terminated_applications AS ta     		ON fst.application_id = ta.application_id
  LEFT JOIN applications_backfill_step_2 AS ab      	ON fst.application_id = ab.application_id
  LEFT JOIN vertical_brand AS vb               		ON COALESCE(ab.ally_slug, fst.ally_slug) = vb.ally_slug
  LEFT JOIN preapproval_flagged_applications pfa    	ON fst.application_id = pfa.application_id
)
,
funnel_step_2 AS (
	SELECT
		fs1.application_id,
        fs1.client_id,
        fs1.client_type,
		fs1.channel,
        fs1.ally_slug,
		fs1.brand,
		fs1.vertical,
		fs1.journey_name,
		fs1.journey_stage_name,
        fs1.flag_preapproval,
        MAX(fs1.flag_approval_event) AS flag_approval_event,
		GREATEST(MIN(fs1.flag_out),MAX(fs1.flag_approval_event)) AS flag_out,
		MAX(COALESCE(fs1.ocurred_on_approval,fs1.ocurred_on_creation)) AS ocurred_on_timestamp,
		MAX(fs1.ocurred_on_approval) AS ocurred_on_approval,
		MIN(fs1.ocurred_on_creation) AS ocurred_on_creation
	FROM funnel_step_1 fs1
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,
funnel_step_2_complement AS (
	SELECT
		*,
		COLLECT_SET(NAMED_STRUCT("application_id",application_id,
								 "flag_approval_event", flag_approval_event,
								 "ocurred_on_timestamp", ocurred_on_timestamp,
								 "ocurred_on_approval",ocurred_on_approval,
								 "ocurred_on_creation",ocurred_on_creation,
								 "channel", channel,
								 "flag_preapproval",flag_preapproval))
		OVER (PARTITION BY client_id,
						   journey_name,
						   to_date(ocurred_on_timestamp),
						   ally_slug) AS applications_array
	FROM funnel_step_2
)
,
funnel_step_3 AS (
    SELECT
        client_id,
        ally_slug,
        journey_name,
        journey_stage_name,
        client_type,
		brand,
		vertical,
		TO_DATE(ocurred_on_timestamp) AS ocurred_on_date,
		CASE
			WHEN journey_name IN ('PREAPPROVAL_PAGO_BR', 'PREAPPROVAL_CHECKPOINT_PAGO_BR') THEN 'BR-PREAPPROVAL'
			WHEN journey_name IN ('PROSPECT_PAGO_BR', 'PROSPECT_PAGO_V2_BR', 'PROSPECT_CHECKPOINT_PAGO_BR', 'CLIENT_PAGO_BR') THEN 'BR-PAGO'
            WHEN journey_name IN ('PROSPECT_PAGO_BNPN_BR', 'BNPN_PIX_BR', 'PROSPECT_PAGO_BNPN_BR_LEGACY') THEN 'BR-BNPN'
		END AS journey_homolog,
	    CASE
			WHEN journey_stage_name IN ('additional-information-br') THEN 'additional-information-br'
			WHEN journey_stage_name IN ('background-check-br') THEN 'background-check-br'
			WHEN journey_stage_name IN ('banking-license-partner-br') THEN 'banking-license-partner-br'
			WHEN journey_stage_name IN ('basic-identity-br') THEN 'basic-identity-br'
			WHEN journey_stage_name IN ('bn-pn-payments-br') THEN 'bn-pn-payments-br'
			WHEN journey_stage_name IN ('cellphone-validation-br') THEN 'cellphone-validation-br'
			WHEN journey_stage_name IN ('device-information') THEN 'device-information-br'
			WHEN journey_stage_name IN ('down-payment-br') THEN 'down-payment-br'
			WHEN journey_stage_name IN ('fraud-check-br','fraud-check-rc-br') THEN 'fraud-check-br'
			WHEN journey_stage_name IN ('identity-photos','identity-wa') THEN 'identity-verification-br'
			WHEN journey_stage_name IN ('idv-third-party') THEN 'idv-third-party-br'
			WHEN journey_stage_name IN ('loan-acceptance-br') THEN 'loan-acceptance-br'
			WHEN journey_stage_name IN ('loan-proposals-br', 'loan-proposals-down-payment-br') THEN 'loan-proposals-br'
			WHEN journey_stage_name IN ('personal-information-br') THEN 'personal-information-br'
			WHEN journey_stage_name IN ('preapproval-summary-br') THEN 'preapproval-summary-br'
			WHEN journey_stage_name IN ('preconditions-br') THEN 'preconditions-br'
			WHEN journey_stage_name IN ('privacy-policy-br','privacy-policy-v2-br') THEN 'privacy-policy-br'
			WHEN journey_stage_name IN ('psychometric_assessment') THEN 'psychometric-assessment-br'
			WHEN journey_stage_name IN ('subproduct-selection-br') THEN 'subproduct-selection-br'
			WHEN journey_stage_name IN ('underwriting-br', 'underwriting-preapproval-br', 'underwriting-rc-br') THEN 'underwriting-br'
	  	END AS journey_stage_homolog,
	  	MAX(ocurred_on_approval) AS ocurred_on_approval,
		MIN(ocurred_on_creation) AS ocurred_on_creation,
		MAX(flag_approval_event) AS flag_approval_event,
		MAX(flag_out) AS flag_out,
        MAX(flag_preapproval) AS flag_preapproval,
		FIRST(applications_array) AS applications_array,
        --COLLECT_LIST(NAMED_STRUCT("application_id",application_id, "flag_approval_event", flag_approval_event, "ocurred_on_timestamp", ocurred_on_timestamp,"ocurred_on_approval",ocurred_on_approval,"ocurred_on_creation",ocurred_on_creation, "channel", channel,"flag_preapproval",flag_preapproval)) AS applications_array,
		1 AS flag_in
	FROM funnel_step_2_complement
	GROUP BY 1,2,3,4,5,6,7,8,9,10
)
,
funnel_step_4 AS (
	SELECT
        client_id,
        ally_slug,
		journey_name,
		journey_homolog,
        client_type,
		brand,
		vertical,
		ocurred_on_date,
		COALESCE(MAX(ocurred_on_approval), MIN(ocurred_on_creation)) AS	ocurred_on_timestamp,
		MAX(CASE WHEN journey_stage_homolog IN ('additional-information-br', 'additional-information-v2-br') THEN flag_in ELSE 0 END) AS additional_information_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('additional-information-br', 'additional-information-v2-br') THEN flag_out ELSE 0 END) AS additional_information_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('background-check-br') THEN flag_in ELSE 0 END) AS background_check_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('background-check-br') THEN flag_out ELSE 0 END) AS background_check_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('banking-license-partner-br') THEN flag_in ELSE 0 END) AS banking_license_partner_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('banking-license-partner-br') THEN flag_out ELSE 0 END) AS banking_license_partner_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('basic-identity-br') THEN flag_in ELSE 0 END) AS basic_identity_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('basic-identity-br') THEN flag_out ELSE 0 END) AS basic_identity_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('bn-pn-payments-br') THEN flag_in ELSE 0 END) AS bn_pn_payments_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('bn-pn-payments-br') THEN flag_out ELSE 0 END) AS bn_pn_payments_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('cellphone-validation-br') THEN flag_in ELSE 0 END) AS cellphone_validation_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('cellphone-validation-br') THEN flag_out ELSE 0 END) AS cellphone_validation_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('device-information-br') THEN flag_in ELSE 0 END) AS device_information_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('device-information-br') THEN flag_out ELSE 0 END) AS device_information_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('down-payment-br') THEN flag_in ELSE 0 END) AS down_payment_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('down-payment-br') THEN flag_out ELSE 0 END) AS down_payment_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('fraud-check-br') THEN flag_in ELSE 0 END) AS fraud_check_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('fraud-check-br') THEN flag_out ELSE 0 END) AS fraud_check_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('identity-verification-br') THEN flag_in ELSE 0 END) AS identity_verification_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('identity-verification-br') THEN flag_out ELSE 0 END) AS identity_verification_br_out,
        MAX(CASE WHEN journey_stage_homolog IN ('idv-third-party-br') THEN flag_in ELSE 0 END) AS idv_third_party_br_in,
        MAX(CASE WHEN journey_stage_homolog IN ('idv-third-party-br') THEN flag_out ELSE 0 END) AS idv_third_party_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-acceptance-br') THEN flag_in ELSE 0 END) AS loan_acceptance_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-acceptance-br') THEN flag_out ELSE 0 END) AS loan_acceptance_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-proposals-br') THEN flag_in ELSE 0 END) AS loan_proposals_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-proposals-br') THEN flag_out ELSE 0 END) AS loan_proposals_br_out,
   		MAX(CASE WHEN journey_stage_homolog IN ('personal-information-br') THEN flag_in ELSE 0 END) AS personal_information_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('personal-information-br') THEN flag_out ELSE 0 END) AS personal_information_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('preapproval-summary-br') THEN flag_in ELSE 0 END) AS preapproval_summary_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('preapproval-summary-br') THEN flag_out ELSE 0 END) AS preapproval_summary_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('preconditions-br') THEN flag_in ELSE 0 END) AS preconditions_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('preconditions-br') THEN flag_out ELSE 0 END) AS preconditions_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('privacy-policy-br', 'privacy-policy-v2-br') THEN flag_in ELSE 0 END) AS privacy_policy_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('privacy-policy-br', 'privacy-policy-v2-br') THEN flag_out ELSE 0 END) AS privacy_policy_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('psychometric-assessment-br') THEN flag_in ELSE 0 END) AS psychometric_assessment_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('psychometric-assessment-br') THEN flag_out ELSE 0 END) AS psychometric_assessment_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('subproduct-selection-br') THEN flag_in ELSE 0 END) AS subproduct_selection_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('subproduct-selection-br') THEN flag_out ELSE 0 END) AS subproduct_selection_br_out,
		MAX(CASE WHEN journey_stage_homolog IN ('underwriting-br') THEN flag_in ELSE 0 END) AS underwriting_br_in,
		MAX(CASE WHEN journey_stage_homolog IN ('underwriting-br') THEN flag_out ELSE 0 END) AS underwriting_br_out,
        MAX(flag_approval_event) AS flag_approval_event,
		MAX(flag_preapproval) AS flag_preapproval,
        FIRST(applications_array) AS applications_array
	FROM funnel_step_3
	WHERE journey_homolog IS NOT NULL
  		--AND ally_name IS NOT NULL
  		--AND client_id IS NOT NULL
  	GROUP BY 1,2,3,4,5,6,7,8
)
SELECT
	md5(CONCAT(fs4.ocurred_on_date, fs4.client_id, fs4.ally_slug, fs4.journey_name)) AS application_process_id,
	fs4.ocurred_on_date,
	fs4.ocurred_on_timestamp,
    fs4.client_id,
	fs4.journey_name,
	fs4.journey_homolog,
	fs4.ally_slug,
	fs4.brand,
	fs4.vertical,
	fs4.flag_approval_event,
	ELEMENT_AT(ARRAY_SORT(fs4.applications_array, (LEFT, RIGHT) -> CASE
            WHEN LEFT.ocurred_on_timestamp < RIGHT.ocurred_on_timestamp THEN 1
            WHEN LEFT.ocurred_on_timestamp > RIGHT.ocurred_on_timestamp THEN -1
            WHEN LEFT.ocurred_on_timestamp == RIGHT.ocurred_on_timestamp THEN 0
          	END), 1).channel as channel,
	fs4.client_type,
	COALESCE(fs4.flag_preapproval, 0) AS flag_preapproval,
	fs4.additional_information_br_in,
	fs4.additional_information_br_out,
	fs4.background_check_br_in,
	fs4.background_check_br_out,
	fs4.banking_license_partner_br_in,
	fs4.banking_license_partner_br_out,
	fs4.basic_identity_br_in,
	fs4.basic_identity_br_out,
	fs4.bn_pn_payments_br_in,
	fs4.bn_pn_payments_br_out,
	fs4.cellphone_validation_br_in,
	fs4.cellphone_validation_br_out,
	fs4.device_information_br_in,
	fs4.device_information_br_out,
	fs4.down_payment_br_in,
	fs4.down_payment_br_out,
	fs4.fraud_check_br_in,
	fs4.fraud_check_br_out,
	fs4.identity_verification_br_in,
	fs4.identity_verification_br_out,
	fs4.idv_third_party_br_in,
	fs4.idv_third_party_br_out,
	fs4.loan_acceptance_br_in,
	fs4.loan_acceptance_br_out,
	fs4.loan_proposals_br_in,
	fs4.loan_proposals_br_out,
	fs4.personal_information_br_in,
	fs4.personal_information_br_out,
	fs4.preapproval_summary_br_in,
	fs4.preapproval_summary_br_out,
	fs4.preconditions_br_in,
	fs4.preconditions_br_out,
	fs4.privacy_policy_br_in,
	fs4.privacy_policy_br_out,
	fs4.psychometric_assessment_br_in,
	fs4.psychometric_assessment_br_out,
	fs4.subproduct_selection_br_in,
	fs4.subproduct_selection_br_out,
	fs4.underwriting_br_in,
	fs4.underwriting_br_out,
    fs4.applications_array,
	SIZE(applications_array) AS num_applications
FROM funnel_step_4 AS fs4