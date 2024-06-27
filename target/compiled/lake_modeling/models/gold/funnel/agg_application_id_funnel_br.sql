

WITH data_with_array_struct AS (
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
        flag_preapproval,
        flag_approval_event,
        ARRAY(
            NAMED_STRUCT('stage','application_process_start','values',NAMED_STRUCT('in',1,'out',1),'priority',-1),
            NAMED_STRUCT('stage','additional_information_br','values',NAMED_STRUCT('in',additional_information_br_in,'out',additional_information_br_out),'priority',13),
            NAMED_STRUCT('stage','background_check_br','values',NAMED_STRUCT('in',background_check_br_in,'out',background_check_br_out),'priority',15),
            NAMED_STRUCT('stage','banking_license_partner_br','values',NAMED_STRUCT('in',banking_license_partner_br_in,'out',banking_license_partner_br_out),'priority',65),
            NAMED_STRUCT('stage','basic_identity_br','values',NAMED_STRUCT('in',basic_identity_br_in,'out',basic_identity_br_out),'priority',20),
            NAMED_STRUCT('stage','bn_pn_payments_br','values',NAMED_STRUCT('in',bn_pn_payments_br_in,'out',bn_pn_payments_br_out),'priority',75),
            NAMED_STRUCT('stage','cellphone_validation_br','values',NAMED_STRUCT('in',cellphone_validation_br_in,'out',cellphone_validation_br_out),'priority',11),
            NAMED_STRUCT('stage','device_information_br','values',NAMED_STRUCT('in',device_information_br_in,'out',device_information_br_out),'priority',25),
            NAMED_STRUCT('stage','down_payment_br','values',NAMED_STRUCT('in',down_payment_br_in,'out',down_payment_br_out),'priority',60),
            NAMED_STRUCT('stage','fraud_check_br','values',NAMED_STRUCT('in',fraud_check_br_in,'out',fraud_check_br_out),'priority',35),
            NAMED_STRUCT('stage','identity_verification_br','values',NAMED_STRUCT('in',identity_verification_br_in,'out',identity_verification_br_out),'priority',45),
            NAMED_STRUCT('stage','idv_third_party_br','values',NAMED_STRUCT('in',idv_third_party_br_in,'out',idv_third_party_br_out),'priority',43),
            NAMED_STRUCT('stage','loan_acceptance_br','values',NAMED_STRUCT('in',loan_acceptance_br_in,'out',loan_acceptance_br_out),'priority',50),
            NAMED_STRUCT('stage','loan_proposals_br','values',NAMED_STRUCT('in',loan_proposals_br_in,'out',loan_proposals_br_out),'priority',40),
            NAMED_STRUCT('stage','personal_information_br','values',NAMED_STRUCT('in',personal_information_br_in,'out',personal_information_br_out),'priority',12),
            NAMED_STRUCT('stage','preapproval_summary_br','values',NAMED_STRUCT('in',preapproval_summary_br_in,'out',preapproval_summary_br_out),'priority',55),
            NAMED_STRUCT('stage','preconditions_br','values',NAMED_STRUCT('in',preconditions_br_in,'out',preconditions_br_out),'priority',5),
            NAMED_STRUCT('stage','privacy_policy_br','values',NAMED_STRUCT('in',privacy_policy_br_in,'out',privacy_policy_br_out),'priority',10),
            NAMED_STRUCT('stage','psychometric_assessment_br','values',NAMED_STRUCT('in',psychometric_assessment_br_in,'out',psychometric_assessment_br_out),'priority',35),
            NAMED_STRUCT('stage','subproduct_selection_br','values',NAMED_STRUCT('in',subproduct_selection_br_in,'out',subproduct_selection_br_out),'priority',70),
            NAMED_STRUCT('stage','underwriting_br','values',NAMED_STRUCT('in',underwriting_br_in,'out',underwriting_br_out),'priority',30),
            NAMED_STRUCT('stage','E2E','values',NAMED_STRUCT('in',1,'out',flag_approval_event),'priority',0)
        ) AS stages_struct_array
    FROM gold.dm_application_id_funnel_br
),

data_with_array_struct_and_filter AS (
    SELECT
        *,
        FILTER(stages_struct_array, struct_ -> struct_.values.in == 1) AS filtered_stages
    FROM data_with_array_struct
),

data_exploded AS (
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
        flag_preapproval,
        flag_approval_event,
        explode(filtered_stages) AS stage_data
    FROM data_with_array_struct_and_filter
),

data_exploded_to_values (
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
        flag_preapproval,
        flag_approval_event,
        stage_data.stage AS stage,
        stage_data.values.in AS stage_in,
        stage_data.values.out AS stage_out,
        stage_data.priority as stage_priority_proxy
    FROM data_exploded
)

SELECT
    'BR' as country_code,
    ocurred_on_date,
    HOUR(ocurred_on_timestamp) AS ocurred_on_hour,
    DATE_TRUNC('HOUR', ocurred_on_timestamp) AS ocurred_on_date_hour,
    journey_name,
    journey_homolog,
    brand,
    ally_slug,
    vertical,
    client_type,
    stage,
    stage_priority_proxy,
    count(1) AS num_application_process,
    sum(flag_approval_event) AS sum_flag_approval_event,
    sum(stage_in) AS sum_stage_in,
    sum(stage_out) AS sum_stage_out,
    FIRST_VALUE(application_process_id) AS exemplar_application_process_id,
    FIRST_VALUE(application_id) AS exemplar_application_id
FROM data_exploded_to_values

    WHERE DATE_TRUNC('HOUR', ocurred_on_timestamp) >= (SELECT MAX(ocurred_on_date_hour) FROM gold.agg_application_id_funnel_br)

GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12