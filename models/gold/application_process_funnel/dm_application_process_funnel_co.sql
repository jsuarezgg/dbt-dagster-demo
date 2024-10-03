{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}
{#- List of journey stage homologs to instantiate columns, one flag for in and one flag for out per case-#}
{%- set journey_stage_homolog_lst = [
        'additional-information-co',
        'background-check-co',
        'basic-identity-co',
        'bnpn-summary-co',
        'cellphone-validation-co',
        'device-information-co',
        'email-verification-co',
        'face-verification-co',
        'fraud-check-co',
        'identity-verification-co',
        'loan-acceptance-co',
        'loan-acceptance-v2-co',
        'loan-proposals-co',
        'personal-information-co',
        'preapproval-summary-co',
        'preapproval-survey-co',
        'preconditions-co',
        'preconditions-refinance-pago-co',
        'privacy-policy-co',
        'privacy-policy-v2-co',
        'pse-payment-co',
        'psychometric-assessment-co',
        'refinance-loan-acceptance-co',
        'refinance-loan-proposal-selection-co',
        'refinance-loan-proposals-co',
        'risk-evaluation-santander-co',
        'underwriting-co',
        'work-information-co']
%}
-- DEBUG: Compiled with `{{journey_stage_homolog_lst|length}}` elements -> `journey_stage_homolog_lst`: {{journey_stage_homolog_lst}}
WITH
f_origination_events_co_logs_filtered AS (
    SELECT *
    FROM {{ source('silver_live', 'f_origination_events_co_logs') }}
    WHERE application_id IS NOT NULL
)
,
stg_funnel_step1_application_process_at_application_id_values_co_finished_only AS (
    SELECT *
    FROM {{ ref('stg_funnel_step1_application_process_at_application_id_values_co') }}
    WHERE has_finished
)
,
stg_funnel_step2_application_process_representation_co AS (
    SELECT *
    FROM {{ ref('stg_funnel_step2_application_process_representation_co') }}
)
,
aux_funnel_application_journey_stage_grouping_co AS (
    SELECT *
    FROM {{ ref('aux_funnel_application_journey_stage_grouping') }}
    WHERE country_code = 'CO'
)
,
application_id_to_application_process_id_co AS (
    -- Not using `bl_application_id_to_application_process_id_co` to avoid creating dependencies on that source
    --           for this use case, but basically we're doing the same
    SELECT
        EXPLODE(debug_application_ids_array.application_id) AS application_id, --Explodes the list of application_id
        application_process_id
        --,* -- Not interested in other fields
    FROM  stg_funnel_step2_application_process_representation_co
)
,
custom_privacy_policy_screens AS (
	SELECT
	    -- LEVEL 2 - application_id-wise custom fields
		application_id,
		1 AS privacy_policy_stage_in,
		MAX(CASE WHEN event_name = 'ClientFirstLastNameConfirmedCO' THEN 1 ELSE 0 END) AS privacy_first_name_co_out,
		MAX(CASE WHEN event_name = 'ClientNationalIdentificationExpeditionDateConfirmedCO' THEN 1 ELSE 0 END) AS privacy_expiration_date_co_out,
		MAX(CASE WHEN event_name = 'PrivacyPolicyAcceptedCO' THEN 1 ELSE 0 END) AS privacy_accepted_co_out
	FROM f_origination_events_co_logs_filtered
	WHERE journey_stage_name IN ('privacy-policy-co') AND ocurred_on > '2022-07-29 19:50:00' -- First inners steps events
	GROUP BY 1
)
,
dm_funnel_step_1 AS (
    -- Get data at the event_id level
    SELECT
        -- LEVEL 2 - application_id-wise fields + custom fields
        oef.application_id,
        stg_1.has_reached_approval::INT AS application_has_reached_approval_int,
        -- LEVEL 1 - Journey-stage-wise fields + custom fields
        oef.journey_stage_name,
        jsg.journey_stage_homolog,
        -- LEVEL 0 - Event-wise fields + custom fields
        oef.event_id,
        oef.event_name,
        oef.event_type,
        CASE WHEN oef.event_type IN ('DECLINATION','ABANDONMENT','REJECTION') THEN 0 ELSE 1 END AS event_flag_out
    FROM       f_origination_events_co_logs_filtered                                           AS oef
    -- IMPORTANT: INNER JOIN: ONLY WORK ON APPLICATIONS WITH PRE-CALCULATED VALUES / LOGIC AND CONSIDERED AS FINISHED
    INNER JOIN stg_funnel_step1_application_process_at_application_id_values_co_finished_only AS stg_1 ON oef.application_id     = stg_1.application_id
    LEFT  JOIN aux_funnel_application_journey_stage_grouping_co                               AS jsg   ON oef.journey_stage_name = jsg.journey_stage_name
)
,
dm_funnel_step_2 AS (
    -- Aggregate data at the journey_stage level (homolog actually)
    SELECT
        -- LEVEL 2 - application_id-wise fields
        application_id,
        -- LEVEL 1 - Journey-stage-wise fields and custom fields
        journey_stage_homolog,
        GREATEST( MIN(event_flag_out), MAX(application_has_reached_approval_int)) AS journey_stage_flag_out, -- The greatest of: if stopped at a stage or the application was approved
        MAX(1) AS journey_stage_flag_in -- DEFAULT TO 1
    FROM     dm_funnel_step_1
    GROUP BY application_id, journey_stage_homolog
)
,
dm_funnel_step_3 AS (
    -- Aggregate data at the application_id level  -- Side note: Jinja padding to left if this one doesn't work: "%-40s"|format(value_quoted)
    SELECT
        -- LEVEL 2 - application_id-wise fields
        fs_2.application_id,
        -- CUSTOM STEPS IN & OUTS
        FIRST_VALUE(cpps.privacy_policy_stage_in, TRUE) AS privacy_policy_stage_in,
        FIRST_VALUE(cpps.privacy_first_name_co_out, TRUE) AS privacy_first_name_co_out,
        FIRST_VALUE(cpps.privacy_expiration_date_co_out, TRUE) AS privacy_expiration_date_co_out,
        FIRST_VALUE(cpps.privacy_accepted_co_out, TRUE) AS privacy_accepted_co_out,
        -- STANDARD JOURNEY STAGES IN & OUTS - For every journey_stage_homolog create a couple fields: <value_no_dash>_in & <value_no_dash>_out
        {%- for value in journey_stage_homolog_lst -%}
        {%- set value_no_dash = value | replace('-', '_') -%}{%- set value_quoted = "'" ~ value ~ "'" %}
        MAX(CASE WHEN fs_2.journey_stage_homolog = {{ "{:<40}".format(value_quoted) }} THEN fs_2.journey_stage_flag_in  ELSE 0 END) AS {{ value_no_dash }}_in,
        MAX(CASE WHEN fs_2.journey_stage_homolog = {{ "{:<40}".format(value_quoted) }} THEN fs_2.journey_stage_flag_out ELSE 0 END) AS {{ value_no_dash }}_out{%- if not loop.last -%},{%- endif -%}
        {%- endfor %}
    FROM      dm_funnel_step_2 AS fs_2
    LEFT JOIN custom_privacy_policy_screens AS cpps ON fs_2.application_id = cpps.application_id
    GROUP BY  fs_2.application_id
)
,
dm_funnel_step_4 AS (
    -- Aggregate data now at the application_process_id level
    SELECT
        -- LEVEL 4 - application_process_id-wise fields (Level 3 is application_process_baseline_id, which we are no longer interested in)
        ap_ai.application_process_id,
        -- CUSTOM STEPS IN & OUTS
        MAX(fs_3.privacy_policy_stage_in) AS privacy_policy_stage_in,
        MAX(fs_3.privacy_first_name_co_out) AS privacy_first_name_co_out,
        MAX(fs_3.privacy_expiration_date_co_out) AS privacy_expiration_date_co_out,
        MAX(fs_3.privacy_accepted_co_out) AS privacy_accepted_co_out,
        -- STANDARD JOURNEY STAGES IN & OUTS
        {%- for value in journey_stage_homolog_lst -%}
        {%- set value_no_dash = value | replace('-', '_') %}
        MAX(fs_3.{{ value_no_dash }}_in) AS {{ value_no_dash }}_in,
        MAX(fs_3.{{ value_no_dash }}_out) AS {{ value_no_dash }}_out{%- if not loop.last -%},{%- endif -%}
        {%- endfor %}
    FROM       dm_funnel_step_3                            AS fs_3
    LEFT JOIN  application_id_to_application_process_id_co AS ap_ai ON fs_3.application_id = ap_ai.application_id
    GROUP BY   ap_ai.application_process_id
)

SELECT
    -- PRIMARY KEY - new concept
    fs_4.application_process_id,
    -- Bring all relevant data, pre-calculated at the application_process_id level
    -- Renaming them to not break any query / data product. Same approach as with `dm_originations` refactor
    stg_2.client_id,
    stg_2.ally_slugs_string AS ally_slug,
    stg_2.synthetic_journey_name AS journey_name,
    stg_2.custom_day_origination_ordinal,
    stg_2.synthetic_ocurred_on_date_local AS ocurred_on_date,
    stg_2.synthetic_ocurred_on_local AS ocurred_on_timestamp,
    stg_2.has_reached_approval::INT AS flag_approval_event,
    stg_2.is_using_preapproval_proxy::INT AS flag_preapproval,
    stg_2.journey_homolog,
    stg_2.term,
    stg_2.client_type,
    stg_2.channel,
    COALESCE(stg_2.ally_clusters_string,'KA') AS ally_cluster, --WITH DEFAULT
    stg_2.ally_brands_string AS brand,
    stg_2.ally_verticals_string AS vertical,
    stg_2.account_kam_names_string AS account_kam_name,
    stg_2.synthetic_product_category,
    stg_2.synthetic_product_subcategory,
    stg_2.is_addishop_referral_with_default AS is_addishop_referral,
    stg_2.is_addishop_referral_paid_with_default AS is_addishop_referral_paid,
    stg_2.debug_num_unique_applications,
    stg_2.debug_sum_has_reached_approval,
    -- CUSTOM STEPS IN & OUTS
    fs_4.privacy_policy_stage_in,
    fs_4.privacy_first_name_co_out,
    fs_4.privacy_expiration_date_co_out,
    fs_4.privacy_accepted_co_out,
    -- STANDARD JOURNEY STAGES IN & OUTS
    {%- for value in journey_stage_homolog_lst -%}
    {%- set value_no_dash = value | replace('-', '_') %}
    fs_4.{{ value_no_dash }}_in,
    fs_4.{{ value_no_dash }}_out,
    {%- endfor %}
    -- DATA PLATFORM COLUMNS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
FROM       dm_funnel_step_4                                       AS fs_4
LEFT JOIN  stg_funnel_step2_application_process_representation_co AS stg_2 ON fs_4.application_process_id = stg_2.application_process_id
-- WHERE NOTE: this filter ignores 2 legacy EXPEDITING journeys (~1k Application ids), to prevent ignoring brand new
-- journeys make sure to include them in the model `gold_staging.aux_funnel_application_journey_grouping`
WHERE stg_2.journey_homolog IS NOT NULL