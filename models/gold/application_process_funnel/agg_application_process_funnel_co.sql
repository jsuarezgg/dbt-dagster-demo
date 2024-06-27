{{
    config(
        materialized='table',
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

WITH data_with_array_struct AS (
    SELECT
        application_process_id,
        ocurred_on_date,
        ocurred_on_timestamp,
        client_id,
        journey_name,
        journey_homolog,
        ally_slug,
        brand,
        vertical,
        ally_cluster,
        channel,
        client_type,
        flag_preapproval,
        flag_approval_event,
        --applications_array,
        term,
        synthetic_product_category,
        synthetic_product_subcategory,
        debug_num_unique_applications AS num_apps,
        ARRAY(
            NAMED_STRUCT('stage','application_process_start','values',NAMED_STRUCT('in',1,'out',1),'priority',-1),
            NAMED_STRUCT('stage','additional_information_co','values',NAMED_STRUCT('in',additional_information_co_in,'out',additional_information_co_out),'priority',48),
            NAMED_STRUCT('stage','background_check_co','values',NAMED_STRUCT('in',background_check_co_in,'out',background_check_co_out),'priority',17),
            NAMED_STRUCT('stage','basic_identity_co','values',NAMED_STRUCT('in',basic_identity_co_in,'out',basic_identity_co_out),'priority',20),
            NAMED_STRUCT('stage','bnpn_summary_co','values',NAMED_STRUCT('in',bnpn_summary_co_in,'out',bnpn_summary_co_out),'priority',10),
            NAMED_STRUCT('stage','cellphone_validation_co','values',NAMED_STRUCT('in',cellphone_validation_co_in,'out',cellphone_validation_co_out),'priority',15),
            NAMED_STRUCT('stage','device_information_co','values',NAMED_STRUCT('in',device_information_co_in,'out',device_information_co_out),'priority',25),
            NAMED_STRUCT('stage','email_verification_co','values',NAMED_STRUCT('in',email_verification_co_in,'out',email_verification_co_out),'priority',55),
            NAMED_STRUCT('stage','face_verification_co','values',NAMED_STRUCT('in',face_verification_co_in,'out',face_verification_co_out),'priority',60),
            NAMED_STRUCT('stage','fraud_check_co','values',NAMED_STRUCT('in',fraud_check_co_in,'out',fraud_check_co_out),'priority',35),
            NAMED_STRUCT('stage','identity_verification_co','values',NAMED_STRUCT('in',identity_verification_co_in,'out',identity_verification_co_out),'priority',45),
            NAMED_STRUCT('stage','loan_acceptance_co','values',NAMED_STRUCT('in',loan_acceptance_co_in,'out',loan_acceptance_co_out),'priority',50),
            NAMED_STRUCT('stage','loan_acceptance_v2_co','values',NAMED_STRUCT('in',loan_acceptance_v2_co_in,'out',loan_acceptance_v2_co_out),'priority',51),
            NAMED_STRUCT('stage','refinance_loan_proposal_selection_co','values',NAMED_STRUCT('in',refinance_loan_proposal_selection_co_in,'out',refinance_loan_proposal_selection_co_out),'priority',39),
            NAMED_STRUCT('stage','preconditions_refinance_pago_co','values',NAMED_STRUCT('in',preconditions_refinance_pago_co_in,'out',preconditions_refinance_pago_co_out),'priority',4),
            NAMED_STRUCT('stage','refinance_loan_acceptance_co','values',NAMED_STRUCT('in',refinance_loan_acceptance_co_in,'out',refinance_loan_acceptance_co_out),'priority',52),
            NAMED_STRUCT('stage','refinance_loan_proposals_co','values',NAMED_STRUCT('in',refinance_loan_proposals_co_in,'out',refinance_loan_proposals_co_out),'priority',41),
            NAMED_STRUCT('stage','loan_proposals_co','values',NAMED_STRUCT('in',loan_proposals_co_in,'out',loan_proposals_co_out),'priority',40),
            NAMED_STRUCT('stage','personal_information_co','values',NAMED_STRUCT('in',personal_information_co_in,'out',personal_information_co_out),'priority',16),
            NAMED_STRUCT('stage','preapproval_summary_co','values',NAMED_STRUCT('in',preapproval_summary_co_in,'out',preapproval_summary_co_out),'priority',55),
            NAMED_STRUCT('stage','preapproval_survey_co','values',NAMED_STRUCT('in',preapproval_survey_co_in,'out',preapproval_survey_co_out),'priority',54),
            NAMED_STRUCT('stage','preconditions_co','values',NAMED_STRUCT('in',preconditions_co_in,'out',preconditions_co_out),'priority',5),
            NAMED_STRUCT('stage','privacy_policy_co','values',NAMED_STRUCT('in',privacy_policy_co_in,'out',privacy_policy_co_out),'priority',10),
            NAMED_STRUCT('stage','privacy_policy_v2_co','values',NAMED_STRUCT('in',privacy_policy_v2_co_in,'out',privacy_policy_v2_co_out),'priority',14),
            NAMED_STRUCT('stage','pse_payment_co','values',NAMED_STRUCT('in',pse_payment_co_in,'out',pse_payment_co_out),'priority',33),
            NAMED_STRUCT('stage','psychometric_assessment_co','values',NAMED_STRUCT('in',psychometric_assessment_co_in,'out',psychometric_assessment_co_out),'priority',35),
            NAMED_STRUCT('stage','risk_evaluation_santander_co','values',NAMED_STRUCT('in',risk_evaluation_santander_co_in,'out',risk_evaluation_santander_co_out),'priority',59),
            NAMED_STRUCT('stage','underwriting_co','values',NAMED_STRUCT('in',underwriting_co_in,'out',underwriting_co_out),'priority',30),
            NAMED_STRUCT('stage','work_information_co','values',NAMED_STRUCT('in',work_information_co_in,'out',work_information_co_out),'priority',47),
            NAMED_STRUCT('stage','step_privacy_first_name_co','values',NAMED_STRUCT('in',privacy_policy_stage_in,'out',privacy_first_name_co_out),'priority',11),
            NAMED_STRUCT('stage','step_privacy_expiration_date_co','values',NAMED_STRUCT('in',privacy_first_name_co_out,'out',privacy_expiration_date_co_out),'priority',12),
            NAMED_STRUCT('stage','step_privacy_accepted_co','values',NAMED_STRUCT('in',privacy_expiration_date_co_out,'out',privacy_accepted_co_out),'priority',13),
            NAMED_STRUCT('stage','pre_udw_conversion','values',NAMED_STRUCT('in',1,'out',underwriting_co_in),'priority',1),
            NAMED_STRUCT('stage','post_udw_conversion','values',NAMED_STRUCT('in',underwriting_co_in,'out',flag_approval_event),'priority',2)
        ) AS stages_struct_array
    FROM {{ ref('dm_application_process_funnel_co') }}
)
,
data_with_array_struct_and_filter AS (
    SELECT *, FILTER(stages_struct_array, struct_ -> struct_.values.in == 1 ) AS filtered_stages
    FROM data_with_array_struct
)
,
data_exploded AS (
	SELECT
		application_process_id,
		ocurred_on_date,
		ocurred_on_timestamp,
		client_id,
		journey_name,
		journey_homolog,
		ally_slug,
		brand,
		vertical,
        ally_cluster,
		channel,
		client_type,
        term,
        synthetic_product_category,
        synthetic_product_subcategory,
		flag_preapproval,
		flag_approval_event,
		num_apps,
		--TRANSFORM(applications_array, application_struct -> application_struct.application_id ) AS application_ids,
		explode(filtered_stages) AS stage_data
	FROM data_with_array_struct_and_filter
),
data_exploded_to_values AS (
    SELECT  
		application_process_id,
        ocurred_on_date,
        ocurred_on_timestamp,
        client_id,
        journey_name,
        journey_homolog,
        ally_slug,
        brand,
        vertical,
        ally_cluster,
        channel,
        client_type,
        term,
        synthetic_product_category,
        synthetic_product_subcategory,
        flag_preapproval,
        flag_approval_event,
        stage_data.stage AS stage,
        stage_data.values.in AS stage_in,
        stage_data.values.out AS stage_out,
        stage_data.priority as stage_priority_proxy,
        num_apps
        --application_ids
    FROM data_exploded
), 

stages_agg_final_data AS (
SELECT 
    'CO' as country_code,
    ocurred_on_date,
    HOUR(ocurred_on_timestamp) AS ocurred_on_hour, 
    DATE_TRUNC('HOUR', ocurred_on_timestamp) AS ocurred_on_date_hour,
    journey_name,
    journey_homolog,
    flag_preapproval,
    channel,
    brand,
    ally_slug,
    vertical,
    ally_cluster,
    client_type,
    term,
    synthetic_product_category,
    synthetic_product_subcategory,
    stage,
    stage_priority_proxy,
    --count(DISTINCT application_process_id) AS num_application_process,
    count(1) AS num_application_process,
    --count(DISTINCT client_id) AS num_client_id,
    sum(flag_approval_event) AS sum_flag_approval_event,
    sum(stage_in) AS sum_stage_in,
    sum(stage_out) AS sum_stage_out,
    FIRST_VALUE(application_process_id) AS exemplar_application_process_id
    --sum(num_apps) AS sum_num_apps
FROM data_exploded_to_values
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
ORDER BY 1 DESC, 2 DESC
),

pre_udw as (
    SELECT
    country_code,
    ocurred_on_date,
    ocurred_on_hour,
    ocurred_on_date_hour,
    journey_name,
    journey_homolog,
    flag_preapproval,
    channel,
    brand,
    ally_slug,
    vertical,
    ally_cluster,
    client_type,
    term,
    'GENERIC' as synthetic_product_category,
    'GENERIC' as synthetic_product_subcategory,
    stage,
    1 as stage_priority_proxy,
    sum(num_application_process) as num_application_process,
    sum(sum_flag_approval_event) as sum_flag_approval_event,
    sum(sum_stage_in) as sum_stage_in,
    sum(sum_stage_out) as sum_stage_out,
    first(exemplar_application_process_id) as exemplar_application_process_id
  FROM stages_agg_final_data
  WHERE stage = 'pre_udw_conversion'
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)
,

pre_udw_for_e2e as (
    select *
    from stages_agg_final_data
    WHERE stage = 'pre_udw_conversion'
),

post_udw_for_e2e as (
    select *
    from stages_agg_final_data
    WHERE stage = 'post_udw_conversion'
),

e2e as (
    SELECT
    pre.country_code,
    pre.ocurred_on_date,
    pre.ocurred_on_hour,
    pre.ocurred_on_date_hour,
    pre.journey_name,
    pre.journey_homolog,
    pre.flag_preapproval,
    pre.channel,
    pre.brand,
    pre.ally_slug,
    pre.vertical,
    pre.ally_cluster,
    pre.client_type,
    pre.term,
    COALESCE(post.synthetic_product_category, pre.synthetic_product_category) as synthetic_product_category,
    COALESCE(post.synthetic_product_subcategory, pre.synthetic_product_subcategory) as synthetic_product_subcategory,
    'E2E' as stage,
    1 as stage_priority_proxy,
    sum(pre.num_application_process) as num_application_process,
    sum(coalesce(post.sum_flag_approval_event, 0)) as sum_flag_approval_event,
    sum(pre.sum_stage_in) as sum_stage_in,
    sum(coalesce(post.sum_stage_out, 0)) as sum_stage_out,
    first(coalesce(post.exemplar_application_process_id, pre.exemplar_application_process_id)) as exemplar_application_process_id,
    sum(pre.sum_stage_in) as pre_sum_stage_in,
    sum(pre.sum_stage_out) as pre_sum_stage_out,
    sum(coalesce(post.sum_stage_in, 0)) as post_sum_stage_in,
    sum(coalesce(post.sum_stage_out, 0)) as post_sum_stage_out
  FROM pre_udw_for_e2e pre
  LEFT JOIN post_udw_for_e2e post
  ON pre.country_code = post.country_code
  AND pre.ocurred_on_date = post.ocurred_on_date
  AND pre.ocurred_on_hour = post.ocurred_on_hour
  AND pre.journey_name = post.journey_name
  AND pre.journey_homolog = post.journey_homolog
  AND pre.flag_preapproval = post.flag_preapproval
  AND pre.channel = post.channel
  AND coalesce(pre.brand, 'no_brand') = coalesce(post.brand, 'no_brand')
  AND pre.ally_slug = post.ally_slug
  AND coalesce(pre.vertical, 'no_vertical') = coalesce(post.vertical, 'no_vertical')
  AND coalesce(pre.ally_cluster, 'no_cluster') = coalesce(post.ally_cluster, 'no_cluster')
  AND pre.client_type = post.client_type
  AND coalesce(pre.term, 'no_term') = coalesce(post.term, 'no_term')
  and coalesce(pre.synthetic_product_category, 'no_spc') = coalesce(post.synthetic_product_category, 'no_spc')
  and coalesce(pre.synthetic_product_subcategory, 'no_sbpc') = coalesce(post.synthetic_product_subcategory, 'no_sbpc')
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
  ),

final_table AS (
select *,
       null as pre_sum_stage_in,
       null as pre_sum_stage_out,
       null as post_sum_stage_in,
       null as post_sum_stage_out
from stages_agg_final_data
where stage <> 'pre_udw_conversion'
UNION ALL
select *,
       null as pre_sum_stage_in,
       null as pre_sum_stage_out,
       null as post_sum_stage_in,
       null as post_sum_stage_out
from pre_udw
where stage = 'pre_udw_conversion'
UNION ALL
select *
from e2e
)
select *
from final_table;
