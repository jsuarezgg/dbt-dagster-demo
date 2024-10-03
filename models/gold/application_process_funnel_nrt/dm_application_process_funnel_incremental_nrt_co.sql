{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}


WITH applications_backfill_step_1 AS (
	SELECT 
	fst.application_id,
	MIN(fst.ocurred_on) AS min_ocurred_on,
    FIRST_VALUE(fst.ally_slug) AS ally_slug,
    FIRST_VALUE(fst.channel) AS channel,
    FIRST_VALUE(fst.client_id) AS client_id,
    FIRST_VALUE(fst.client_type) AS client_type,
    FIRST_VALUE(fst.journey_name) AS journey_name
    FROM {{ source('silver_live', 'f_origination_events_co_logs') }} fst
    GROUP BY 1
)
,
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
		SELECT
            DISTINCT application_id, 'PROSPECT_FINANCIA_SANTANDER_ADDI_CO' AS journey_name_synthetic
        FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
        WHERE journey_name = 'PROSPECT_FINANCIA_SANTANDER_CO'
        AND journey_stage_name IN ('underwriting-co')
	) AS synthetic 		ON fst.application_id = synthetic.application_id
	LEFT JOIN {{ source('silver_live', 'f_applications_co') }} AS a 		ON fst.application_id = a.application_id
    )
,
application_preapproval_co_filtered AS (
	SELECT
        a.client_id,
        a.application_date,
        a.preapproval_expiration_date,
        a.preapproval_amount
	FROM {{ source('silver_live', 'f_applications_co') }} AS a
	INNER JOIN (
		SELECT
			application_id,
			row_number() over(partition by client_id, application_date::date order by application_date) as rn
		FROM {{ source('silver_live', 'f_applications_co') }}
		WHERE channel = 'PRE_APPROVAL'
          AND custom_is_preapproval_completed IS TRUE
		) AS filtered_apps
    ON a.application_id = filtered_apps.application_id
	WHERE filtered_apps.rn = 1
)
,
preapproval_flagged_applications AS (
	SELECT
		DISTINCT a.application_id,
		1 AS flag_preapproval
	FROM {{ source('silver_live', 'f_applications_co') }} AS a
	INNER JOIN application_preapproval_co_filtered AS pf 	ON a.client_id = pf.client_id
                                                            AND a.application_date > pf.application_date AND a.application_date <= pf.preapproval_expiration_date
    LEFT JOIN applications_backfill_step_2 AS ab            ON a.application_id = ab.application_id
	WHERE a.requested_amount <= pf.preapproval_amount
        AND ab.client_type = 'PROSPECT'
        AND a.journey_name NOT ILIKE '%preap%'
)
,
terminated_applications AS (
	SELECT
		DISTINCT application_id
	FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
	WHERE event_type in ('DECLINATION','ABANDONMENT','REJECTION','APPROVAL')
)
,
/*vertical_brand AS (
	SELECT
		DISTINCT ally_slug,
		UPPER(get_json_object(ally_vertical,'$.name.value')) AS vertical
		,UPPER(get_json_object(ally_brand,'$.name.value')) AS brand
	FROM {{ ref('d_ally_management_stores_allies_co') }}
)
,*/
bl_ally_brand_ally_slug_status_co AS (
    SELECT *
    FROM {{ ref('bl_ally_brand_ally_slug_status') }}
    WHERE country_code = 'CO'
)
,
privacy_policy_screens AS (
	SELECT
		application_id,
		1 AS privacy_policy_stage_in,
		MAX(CASE WHEN event_name = 'ClientFirstLastNameConfirmedCO' THEN 1 ELSE 0 END) AS privacy_first_name_co_out,
		MAX(CASE WHEN event_name = 'ClientNationalIdentificationExpeditionDateConfirmedCO' THEN 1 ELSE 0 END) AS privacy_expiration_date_co_out,
		MAX(CASE WHEN event_name = 'PrivacyPolicyAcceptedCO' THEN 1 ELSE 0 END) AS privacy_accepted_co_out
		FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
		WHERE journey_stage_name IN ('privacy-policy-co') AND ocurred_on > '2022-07-29 19:50:00' -- First inners steps events
	GROUP BY 1
)
,
product_assignation as (
	select abs.application_id,
		   blap.synthetic_product_category,
		   blap.synthetic_product_subcategory,
		   case when orig.application_id is not null then 1 else 0 end as originated,
		   orig.term
	from applications_backfill_step_2 abs
	left join {{ ref('bl_application_product_co')}} blap
	on abs.application_id = blap.application_id
	left join {{ source('silver_live', 'f_originations_bnpl_co') }} orig
	on abs.application_id = orig.application_id
)
,
funnel_step_1 AS (
    SELECT
	  	fst.application_id,
		COALESCE(ab.ally_slug, fst.ally_slug) AS ally_slug,
		COALESCE(ab.client_id, fst.client_id) AS client_id,
		COALESCE(ab.journey_name_synthetic, fst.journey_name) AS journey_name,
		fst.journey_stage_name,
		CASE WHEN COALESCE(ab.client_type, fst.client_type) = 'LEAD' THEN 'PROSPECT' ELSE COALESCE(ab.client_type, fst.client_type) END AS client_type,
		COALESCE(ab.channel, fst.channel) AS channel,
		pfa.flag_preapproval,
        asl.ally_brand AS brand,
        asl.ally_vertical AS vertical,
		COALESCE(asl.ally_cluster,'KA') AS ally_cluster,
		pps.privacy_policy_stage_in,
		pps.privacy_first_name_co_out,
		pps.privacy_expiration_date_co_out,
		pps.privacy_accepted_co_out,
		MAX(CASE WHEN event_type IN ('APPROVAL') THEN 1 ELSE 0 END) OVER (PARTITION BY fst.application_id) AS flag_approval_event,
		CASE WHEN fst.event_type IN ('DECLINATION','ABANDONMENT','REJECTION') THEN 0 ELSE 1 END AS flag_out,
		MIN(from_utc_timestamp(fst.ocurred_on,'America/Bogota')) OVER (PARTITION BY fst.application_id) AS ocurred_on_creation,
		MAX(CASE WHEN fst.event_type = 'APPROVAL' THEN from_utc_timestamp(fst.ocurred_on,'America/Bogota') END) OVER (PARTITION BY fst.application_id) AS ocurred_on_approval,
		pa.synthetic_product_category,
		pa.synthetic_product_subcategory,
		pa.term AS term
	FROM {{ source('silver_live', 'f_origination_events_co_logs') }} AS fst
	INNER JOIN terminated_applications AS ta     			ON fst.application_id = ta.application_id
	LEFT JOIN applications_backfill_step_2 AS ab            ON fst.application_id = ab.application_id
    --LEFT JOIN vertical_brand AS vb               			ON COALESCE(ab.ally_slug, fst.ally_slug) = vb.ally_slug
	LEFT JOIN bl_ally_brand_ally_slug_status_co asl 		ON COALESCE(ab.ally_slug, fst.ally_slug) = asl.ally_slug
	LEFT JOIN preapproval_flagged_applications AS pfa       ON fst.application_id = pfa.application_id
	LEFT JOIN privacy_policy_screens AS pps                 ON fst.application_id = pps.application_id
	LEFT JOIN product_assignation AS pa						ON fst.application_id = pa.application_id
)
,
funnel_step_2 AS (
	SELECT
		fs1.application_id,
        fs1.client_id,
		fs1.channel,
        fs1.ally_slug,
		fs1.brand,
		fs1.vertical,
		fs1.ally_cluster,
		fs1.journey_name,
		fs1.journey_stage_name,
        fs1.flag_preapproval,
		fs1.privacy_policy_stage_in,
		fs1.privacy_first_name_co_out,
		fs1.privacy_expiration_date_co_out,
		fs1.privacy_accepted_co_out,
		fs1.term,
		fs1.synthetic_product_category,
		fs1.synthetic_product_subcategory,
		FIRST_VALUE(fs1.client_type) AS client_type,
        MAX(fs1.flag_approval_event) AS flag_approval_event,
		GREATEST(MIN(fs1.flag_out),MAX(fs1.flag_approval_event)) AS flag_out,
		MAX(COALESCE(fs1.ocurred_on_approval,fs1.ocurred_on_creation)) AS ocurred_on_timestamp,
		MAX(fs1.ocurred_on_approval) AS ocurred_on_approval,
		MIN(fs1.ocurred_on_creation) AS ocurred_on_creation
	FROM funnel_step_1 fs1
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
)
,
funnel_step_2_complement AS (
	--2022-10-26 Carlos Puerto N: Fix para casos de agrupación de varias aplicaciones en un mismo application_process pero con stages que puede contengan o no las varias aplicaciones
	-- Campo applications_array precalculado para lograr que todos los registros a nivel de journey_stage_name tengan el mismo objeto de referencia como su lista de aplicaciones para su application_process
	SELECT
		*,
		COLLECT_SET(NAMED_STRUCT("application_id",application_id,
								 "flag_approval_event", flag_approval_event,
								 "ocurred_on_timestamp", ocurred_on_timestamp,
								 "ocurred_on_approval",ocurred_on_approval,
								 "ocurred_on_creation",ocurred_on_creation,
								 "channel", channel,
								 "flag_preapproval",flag_preapproval,
								 "synthetic_product_category",synthetic_product_category,
								 "synthetic_product_subcategory",synthetic_product_subcategory,
								 "client_type",client_type,
								 "term",term))
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
		ally_cluster,
		TO_DATE(ocurred_on_timestamp) AS ocurred_on_date,
		CASE
			WHEN journey_name IN ('PROSPECT_CHECKPOINT_FINANCIA_CO', 'PROSPECT_FINANCIA_CO', 'PROSPECT_FINANCIA_SANTANDER_ADDI_CO', 'PROSPECT_GATEWAY_FINANCIA_CO') THEN 'CO-FINANCIA'
 			WHEN journey_name IN ('PREAPPROVAL_PAGO_CO','PREAPPROVAL_CHECKPOINT_PAGO_CO') THEN 'CO-PREAPPROVAL'
			WHEN journey_name IN ('PROSPECT_FINANCIA_SANTANDER_CO') THEN 'CO-SANTANDER'
            WHEN journey_name IN ('PROSPECT_PAGO_CO', 'PROSPECT_PAGO_V2_CO', 'PROSPECT_CHECKPOINT_PAGO_CO', 'PROSPECT_CHECKPOINT_PAGO_V2_CO',
                                  'CLIENT_PAGO_CO', 'CLIENT_PAGO_V2_CO', 'CLIENT_FINANCIA_CO', 'PROSPECT_GATEWAY_PAGO_CO') THEN 'CO-PAGO'
		END AS journey_homolog,
	    CASE
			WHEN journey_stage_name IN ('additional-information-santander-co') THEN 'additional-information-co'
			WHEN journey_stage_name IN ('background-check-co') THEN 'background-check-co'
			WHEN journey_stage_name IN ('basic-identity-co') THEN 'basic-identity-co'
            WHEN journey_stage_name IN ('cellphone-validation-co') THEN 'cellphone-validation-co'
			WHEN journey_stage_name IN ('device-information') THEN 'device-information-co'
			WHEN journey_stage_name IN ('email-verification') THEN 'email-verification-co'
			WHEN journey_stage_name IN ('face-verification') THEN 'face-verification-co'
			WHEN journey_stage_name IN ('fraud-check-co','fraud-check-rc-co') THEN 'fraud-check-co'
			WHEN journey_stage_name IN ('identity-photos','identity-wa') THEN 'identity-verification-co'
            WHEN journey_stage_name IN ('loan-acceptance-co','loan-acceptance-santander-co') THEN 'loan-acceptance-co'
            WHEN journey_stage_name IN ('loan-acceptance-v2-co') THEN 'loan-acceptance-v2-co'
			WHEN journey_stage_name IN ('loan-proposals-co','loan-proposals-santander-co') THEN 'loan-proposals-co'
            WHEN journey_stage_name IN ('personal-information-co') THEN 'personal-information-co'
			WHEN journey_stage_name IN ('preapproval-summary-co') THEN 'preapproval-summary-co'
			WHEN journey_stage_name IN ('preconditions-co','preconditions-pago-co') THEN 'preconditions-co'
            WHEN journey_stage_name IN ('privacy-policy-co','privacy-policy-santander-co') THEN 'privacy-policy-co'
            WHEN journey_stage_name IN ('privacy-policy-v2-co') THEN 'privacy-policy-v2-co'
			WHEN journey_stage_name IN ('psychometric_assessment') THEN 'psychometric-assessment-co'
			WHEN journey_stage_name IN ('risk-evaluation-santander-co') THEN 'risk-evaluation-santander-co'
			WHEN journey_stage_name IN ('underwriting-co','underwriting-pago-co','underwriting-preapproval-co','underwriting-preapproval-pago-co','underwriting-psychometric-co','underwriting-rc-co','underwriting-rc-pago-co') THEN 'underwriting-co'
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
		FIRST(applications_array) AS applications_array,
		1 AS flag_in
	FROM funnel_step_2_complement
	GROUP BY 1,2,3,4,5,6,7,8,9,10,11
)
,
funnel_step_4 AS (
	SELECT 
		client_id,
        ally_slug,
		journey_name,
		journey_homolog,
		brand,
		vertical,
		ally_cluster,
		ocurred_on_date,
		FIRST_VALUE(client_type) as client_type,
		COALESCE(MAX(ocurred_on_approval), MIN(ocurred_on_creation)) AS	ocurred_on_timestamp,
		MAX(CASE WHEN journey_stage_homolog IN ('additional-information-co') THEN flag_in ELSE 0 END) AS additional_information_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('additional-information-co') THEN flag_out ELSE 0 END) AS additional_information_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('background-check-co') THEN flag_in ELSE 0 END) AS background_check_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('background-check-co') THEN flag_out ELSE 0 END) AS background_check_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('basic-identity-co') THEN flag_in ELSE 0 END) AS basic_identity_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('basic-identity-co') THEN flag_out ELSE 0 END) AS basic_identity_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('cellphone-validation-co') THEN flag_in ELSE 0 END) AS cellphone_validation_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('cellphone-validation-co') THEN flag_out ELSE 0 END) AS cellphone_validation_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('device-information-co') THEN flag_in ELSE 0 END) AS device_information_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('device-information-co') THEN flag_out ELSE 0 END) AS device_information_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('email-verification-co') THEN flag_in ELSE 0 END) AS email_verification_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('email-verification-co') THEN flag_out ELSE 0 END) AS email_verification_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('face-verification-co') THEN flag_in ELSE 0 END) AS face_verification_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('face-verification-co') THEN flag_out ELSE 0 END) AS face_verification_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('fraud-check-co') THEN flag_in ELSE 0 END) AS fraud_check_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('fraud-check-co') THEN flag_out ELSE 0 END) AS fraud_check_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('identity-verification-co') THEN flag_in ELSE 0 END) AS identity_verification_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('identity-verification-co') THEN flag_out ELSE 0 END) AS identity_verification_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-acceptance-co') THEN flag_in ELSE 0 END) AS loan_acceptance_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-acceptance-co') THEN flag_out ELSE 0 END) AS loan_acceptance_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('loan-acceptance-v2-co') THEN flag_in ELSE 0 END) AS loan_acceptance_v2_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-acceptance-v2-co') THEN flag_out ELSE 0 END) AS loan_acceptance_v2_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-proposals-co') THEN flag_in ELSE 0 END) AS loan_proposals_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('loan-proposals-co') THEN flag_out ELSE 0 END) AS loan_proposals_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('personal-information-co') THEN flag_in ELSE 0 END) AS personal_information_co_in,
        MAX(CASE WHEN journey_stage_homolog IN ('personal-information-co') THEN flag_out ELSE 0 END) AS personal_information_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('preapproval-summary-co') THEN flag_in ELSE 0 END) AS preapproval_summary_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('preapproval-summary-co') THEN flag_out ELSE 0 END) AS preapproval_summary_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('preconditions-co') THEN flag_in ELSE 0 END) AS preconditions_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('preconditions-co') THEN flag_out ELSE 0 END) AS preconditions_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('privacy-policy-co') THEN flag_in ELSE 0 END) AS privacy_policy_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('privacy-policy-co') THEN flag_out ELSE 0 END) AS privacy_policy_co_out,
        MAX(CASE WHEN journey_stage_homolog IN ('privacy-policy-v2-co') THEN flag_in ELSE 0 END) AS privacy_policy_v2_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('privacy-policy-v2-co') THEN flag_out ELSE 0 END) AS privacy_policy_v2_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('psychometric-assessment-co') THEN flag_in ELSE 0 END) AS psychometric_assessment_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('psychometric-assessment-co') THEN flag_out ELSE 0 END) AS psychometric_assessment_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('risk-evaluation-santander-co') THEN flag_in ELSE 0 END) AS risk_evaluation_santander_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('risk-evaluation-santander-co') THEN flag_out ELSE 0 END) AS risk_evaluation_santander_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('underwriting-co') THEN flag_in ELSE 0 END) AS underwriting_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('underwriting-co') THEN flag_out ELSE 0 END) AS underwriting_co_out,
		MAX(CASE WHEN journey_stage_homolog IN ('work-information-co') THEN flag_in ELSE 0 END) AS work_information_co_in,
		MAX(CASE WHEN journey_stage_homolog IN ('work-information-co') THEN flag_out ELSE 0 END) AS work_information_co_out,
        MAX(flag_approval_event) AS flag_approval_event,
		MAX(flag_preapproval) AS flag_preapproval,
		MAX(privacy_policy_stage_in) AS privacy_policy_stage_in,
		MAX(privacy_first_name_co_out) AS privacy_first_name_co_out,
		MAX(privacy_accepted_co_out) AS privacy_accepted_co_out,
		MAX(privacy_expiration_date_co_out) AS privacy_expiration_date_co_out,
        FIRST(applications_array) AS applications_array
	FROM funnel_step_3
	WHERE journey_homolog IS NOT NULL
  		--AND ally_slug IS NOT NULL
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
	fs4.ally_cluster,
	ELEMENT_AT(
	ARRAY_SORT(
     	FILTER(fs4.applications_array, x -> x.term IS NOT NULL), -- Filter out null values
		(LEFT, RIGHT) ->
		CASE
			WHEN LEFT.flag_approval_event = 1 AND RIGHT.flag_approval_event = 0 THEN 1
			WHEN LEFT.flag_approval_event = 0 AND RIGHT.flag_approval_event = 1 THEN -1
			WHEN LEFT.flag_approval_event = RIGHT.flag_approval_event THEN
			CASE
				WHEN LEFT.ocurred_on_timestamp < RIGHT.ocurred_on_timestamp THEN -1
				WHEN LEFT.ocurred_on_timestamp > RIGHT.ocurred_on_timestamp THEN 1
				ELSE 0
			END
		END
	),
	-1  -- Retrieve the last element (index -1)
	).term AS term,
	ELEMENT_AT(
	ARRAY_SORT(
     	FILTER(fs4.applications_array, x -> x.synthetic_product_category IS NOT NULL), -- Filter out null values
		(LEFT, RIGHT) ->
		CASE
			WHEN LEFT.flag_approval_event = 1 AND RIGHT.flag_approval_event = 0 THEN 1
			WHEN LEFT.flag_approval_event = 0 AND RIGHT.flag_approval_event = 1 THEN -1
			WHEN LEFT.flag_approval_event = RIGHT.flag_approval_event THEN
			CASE
				WHEN LEFT.ocurred_on_timestamp < RIGHT.ocurred_on_timestamp THEN -1
				WHEN LEFT.ocurred_on_timestamp > RIGHT.ocurred_on_timestamp THEN 1
				ELSE 0
			END
		END
	),
	-1  -- Retrieve the last element (index -1)
	).synthetic_product_category AS synthetic_product_category,
	ELEMENT_AT(
	ARRAY_SORT(
     	FILTER(fs4.applications_array, x -> x.synthetic_product_subcategory IS NOT NULL), -- Filter out null values
		(LEFT, RIGHT) ->
		CASE
			WHEN LEFT.flag_approval_event = 1 AND RIGHT.flag_approval_event = 0 THEN 1
			WHEN LEFT.flag_approval_event = 0 AND RIGHT.flag_approval_event = 1 THEN -1
			WHEN LEFT.flag_approval_event = RIGHT.flag_approval_event THEN
			CASE
				WHEN LEFT.ocurred_on_timestamp < RIGHT.ocurred_on_timestamp THEN -1
				WHEN LEFT.ocurred_on_timestamp > RIGHT.ocurred_on_timestamp THEN 1
				ELSE 0
			END
		END
	),
	-1  -- Retrieve the last element (index -1)
	).synthetic_product_subcategory AS synthetic_product_subcategory,
	ELEMENT_AT(
	ARRAY_SORT(
     	FILTER(fs4.applications_array, x -> x.channel IS NOT NULL), -- Filter out null values
		(LEFT, RIGHT) ->
		CASE
			WHEN LEFT.flag_approval_event = 1 AND RIGHT.flag_approval_event = 0 THEN 1
			WHEN LEFT.flag_approval_event = 0 AND RIGHT.flag_approval_event = 1 THEN -1
			WHEN LEFT.flag_approval_event = RIGHT.flag_approval_event THEN
			CASE
				WHEN LEFT.ocurred_on_timestamp < RIGHT.ocurred_on_timestamp THEN -1
				WHEN LEFT.ocurred_on_timestamp > RIGHT.ocurred_on_timestamp THEN 1
				ELSE 0
			END
		END
	),
	-1  -- Retrieve the last element (index -1)
	).channel AS channel,
	ELEMENT_AT(
	ARRAY_SORT(
     	FILTER(fs4.applications_array, x -> x.client_type IS NOT NULL), -- Filter out null values
		(LEFT, RIGHT) ->
		CASE
			WHEN LEFT.flag_approval_event = 1 AND RIGHT.flag_approval_event = 0 THEN 1
			WHEN LEFT.flag_approval_event = 0 AND RIGHT.flag_approval_event = 1 THEN -1
			WHEN LEFT.flag_approval_event = RIGHT.flag_approval_event THEN
			CASE
				WHEN LEFT.ocurred_on_timestamp < RIGHT.ocurred_on_timestamp THEN -1
				WHEN LEFT.ocurred_on_timestamp > RIGHT.ocurred_on_timestamp THEN 1
				ELSE 0
			END
		END
	),
	-1  -- Retrieve the last element (index -1)
	).client_type AS client_type,
	COALESCE(fs4.flag_preapproval, 0) AS flag_preapproval,
	fs4.flag_approval_event,
	fs4.additional_information_co_in,
	fs4.additional_information_co_out,
	fs4.background_check_co_in,
	fs4.background_check_co_out,
	fs4.basic_identity_co_in,
	fs4.basic_identity_co_out,
    fs4.cellphone_validation_co_in,
    fs4.cellphone_validation_co_out,
	fs4.device_information_co_in,
	fs4.device_information_co_out,
	fs4.email_verification_co_in,
	fs4.email_verification_co_out,
	fs4.face_verification_co_in,
	fs4.face_verification_co_out,
	fs4.fraud_check_co_in,
	fs4.fraud_check_co_out,
	fs4.identity_verification_co_in,
	fs4.identity_verification_co_out,
	fs4.loan_acceptance_co_in,
	fs4.loan_acceptance_co_out,
	fs4.loan_acceptance_v2_co_in,
	fs4.loan_acceptance_v2_co_out,
	fs4.loan_proposals_co_in,
	fs4.loan_proposals_co_out,
    fs4.personal_information_co_in,
    fs4.personal_information_co_out,
	fs4.preapproval_summary_co_in,
	fs4.preapproval_summary_co_out,
	fs4.preconditions_co_in,
	fs4.preconditions_co_out,
	fs4.privacy_policy_stage_in,
	fs4.privacy_accepted_co_out,
	fs4.privacy_expiration_date_co_out,
	fs4.privacy_first_name_co_out,
	fs4.privacy_policy_co_in,
	fs4.privacy_policy_co_out,
	fs4.privacy_policy_v2_co_in,
	fs4.privacy_policy_v2_co_out,
	fs4.psychometric_assessment_co_in,
	fs4.psychometric_assessment_co_out,
	fs4.risk_evaluation_santander_co_in,
	fs4.risk_evaluation_santander_co_out,
	fs4.underwriting_co_in,
	fs4.underwriting_co_out,
	fs4.work_information_co_in,
	fs4.work_information_co_out,
    fs4.applications_array,
	SIZE(applications_array) AS num_applications
FROM funnel_step_4 AS fs4
