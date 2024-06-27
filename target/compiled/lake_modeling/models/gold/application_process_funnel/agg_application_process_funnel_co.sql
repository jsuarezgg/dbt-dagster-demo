

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
        channel,
        client_type,
        flag_preapproval,
        flag_approval_event,
        applications_array,
        ARRAY(
            NAMED_STRUCT('stage','application_process_start','values',NAMED_STRUCT('in',1,'out',1),'priority',-1),
            NAMED_STRUCT('stage','additional_information_co','values',NAMED_STRUCT('in',additional_information_co_in,'out',additional_information_co_out),'priority',48),
            NAMED_STRUCT('stage','background_check_co','values',NAMED_STRUCT('in',background_check_co_in,'out',background_check_co_out),'priority',17),
            NAMED_STRUCT('stage','basic_identity_co','values',NAMED_STRUCT('in',basic_identity_co_in,'out',basic_identity_co_out),'priority',20),
            NAMED_STRUCT('stage','cellphone-validation-co','values',NAMED_STRUCT('in',cellphone_validation_co_in,'out',cellphone_validation_co_out),'priority',15),
            NAMED_STRUCT('stage','device_information_co','values',NAMED_STRUCT('in',device_information_co_in,'out',device_information_co_out),'priority',25),
            NAMED_STRUCT('stage','fraud_check_co','values',NAMED_STRUCT('in',fraud_check_co_in,'out',fraud_check_co_out),'priority',35),
            NAMED_STRUCT('stage','identity_verification_co','values',NAMED_STRUCT('in',identity_verification_co_in,'out',identity_verification_co_out),'priority',45),
            NAMED_STRUCT('stage','loan_acceptance_co','values',NAMED_STRUCT('in',loan_acceptance_co_in,'out',loan_acceptance_co_out),'priority',50),
            NAMED_STRUCT('stage','loan_acceptance_v2_co','values',NAMED_STRUCT('in',loan_acceptance_v2_co_in,'out',loan_acceptance_v2_co_out),'priority',51),
            NAMED_STRUCT('stage','loan_proposals_co','values',NAMED_STRUCT('in',loan_proposals_co_in,'out',loan_proposals_co_out),'priority',40),
            NAMED_STRUCT('stage','personal-information-co','values',NAMED_STRUCT('in',personal_information_co_in,'out',personal_information_co_out),'priority',16),
            NAMED_STRUCT('stage','preapproval_summary_co','values',NAMED_STRUCT('in',preapproval_summary_co_in,'out',preapproval_summary_co_out),'priority',55),
            NAMED_STRUCT('stage','preconditions_co','values',NAMED_STRUCT('in',preconditions_co_in,'out',preconditions_co_out),'priority',5),
            NAMED_STRUCT('stage','privacy_policy_co','values',NAMED_STRUCT('in',privacy_policy_co_in,'out',privacy_policy_co_out),'priority',10),
            NAMED_STRUCT('stage','privacy_policy_v2_co','values',NAMED_STRUCT('in',privacy_policy_v2_co_in,'out',privacy_policy_v2_co_out),'priority',14),
            NAMED_STRUCT('stage','psychometric_assessment_co','values',NAMED_STRUCT('in',psychometric_assessment_co_in,'out',psychometric_assessment_co_out),'priority',35),
            NAMED_STRUCT('stage','underwriting_co','values',NAMED_STRUCT('in',underwriting_co_in,'out',underwriting_co_out),'priority',30),
            NAMED_STRUCT('stage','work_information_co','values',NAMED_STRUCT('in',work_information_co_in,'out',work_information_co_out),'priority',47),
            NAMED_STRUCT('stage','step_privacy_first_name_co','values',NAMED_STRUCT('in',privacy_policy_stage_in,'out',privacy_first_name_co_out),'priority',11),
            NAMED_STRUCT('stage','step_privacy_expiration_date_co','values',NAMED_STRUCT('in',privacy_first_name_co_out,'out',privacy_expiration_date_co_out),'priority',12),
            NAMED_STRUCT('stage','step_privacy_accepted_co','values',NAMED_STRUCT('in',privacy_expiration_date_co_out,'out',privacy_accepted_co_out),'priority',13),
            NAMED_STRUCT('stage','E2E','values',NAMED_STRUCT('in',1,'out',flag_approval_event),'priority',0)
        ) AS stages_struct_array
    FROM gold.dm_application_process_funnel_co
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
		channel,
		client_type,
		flag_preapproval,
		flag_approval_event,
		SIZE(applications_array) AS num_apps,
		TRANSFORM(applications_array, application_struct -> application_struct.application_id ) AS application_ids,
		explode(filtered_stages) AS stage_data
	FROM data_with_array_struct_and_filter
),
data_exploded_to_values (
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
        channel,
        client_type,
        flag_preapproval,
        flag_approval_event,
        stage_data.stage AS stage,
        stage_data.values.in AS stage_in,
        stage_data.values.out AS stage_out,
        stage_data.priority as stage_priority_proxy,
        num_apps,
        application_ids
    FROM data_exploded
)

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
    client_type,
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
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
ORDER BY 1 DESC, 2 DESC