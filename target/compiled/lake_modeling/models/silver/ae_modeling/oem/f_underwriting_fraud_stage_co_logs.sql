
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
conditionalcreditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.creditcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalconditionalcreditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.preapprovalconditionalcreditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedpagoco_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckapprovedsantanderco_co AS ( 
    SELECT *
    FROM bronze.creditcheckapprovedsantanderco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredco_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalconditionalcreditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalconditionalcreditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckfailedpagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckfailedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationapproved_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpsychometricpassedco_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpsychometricpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpsychometricpassedco_co AS ( 
    SELECT *
    FROM bronze.creditcheckpsychometricpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpsychometricfailedco_co AS ( 
    SELECT *
    FROM bronze.creditcheckpsychometricfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedpagoco_co AS ( 
    SELECT *
    FROM bronze.creditcheckfailedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedpagoco_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassedpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredpagoco_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredpagoco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedco_co AS ( 
    SELECT *
    FROM bronze.loanacceptedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,fraudcheckfailedco_co AS ( 
    SELECT *
    FROM bronze.fraudcheckfailedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,fraudcheckpassedco_co AS ( 
    SELECT *
    FROM bronze.fraudcheckpassedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailed_co AS ( 
    SELECT *
    FROM bronze.creditcheckfailed_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassed_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassed_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedwithlowerapprovedamount_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassedwithlowerapprovedamount_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedwithlowerapprovedamount_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedwithlowerapprovedamount_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassed_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassed_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedwithlowerapprovedamount_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedwithlowerapprovedamount_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedwithinsufficientaddicupo_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedwithinsufficientaddicupo_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailed_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailed_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectupgradedtoclient_co AS ( 
    SELECT *
    FROM bronze.prospectupgradedtoclient_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassed_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassed_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedco_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckpassedco_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckfailedco_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalcreditcheckpassedco_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalconditionalcreditcheckpassedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalcreditcheckfailedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,evaluation_type,NULL as fraud_model_score,NULL as fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,NULL as store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckfailedpagoco_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckpassedco_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckfailedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,evaluation_type,NULL as fraud_model_score,NULL as fraud_model_version,group_name,NULL as ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,NULL as store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,NULL as fraud_model_score,NULL as fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,NULL as pd_calculation_method,NULL as policy,NULL as probability_default_addi,NULL as probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,NULL as store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckapprovedsantanderco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM psychometricevaluationisrequiredco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as lbl,NULL as learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalconditionalcreditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalcreditcheckfailedpagoco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalcreditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM psychometricevaluationapproved_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM conditionalcreditcheckpsychometricpassedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckpsychometricpassedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckpsychometricfailedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,NULL as credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,NULL as credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckfailedpagoco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,NULL as credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckpassedpagoco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM psychometricevaluationisrequiredpagoco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM loanacceptedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as lbl,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as policy,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM fraudcheckfailedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as lbl,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as policy,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM fraudcheckpassedco_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_check_income_validator_calculation_method,credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,NULL as pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckfailed_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_check_income_validator_calculation_method,credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM conditionalcreditcheckpassed_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_check_income_validator_calculation_method,credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM creditcheckpassedwithlowerapprovedamount_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_check_income_validator_calculation_method,credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedwithlowerapprovedamount_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_check_income_validator_calculation_method,credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,NULL as learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckpassed_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_check_income_validator_calculation_method,credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,NULL as learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckpassedwithlowerapprovedamount_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,NULL as credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as policy,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckpassedwithinsufficientaddicupo_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as lbl,NULL as learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckfailed_co
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_check_income_validator_calculation_method,NULL as credit_check_income_validator_contribution_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as evaluation_type,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as lbl,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as policy,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_be_black_listed,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM prospectupgradedtoclient_co
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_check_income_validator_calculation_method,credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,NULL as evaluation_type,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,NULL as policy,probability_default_addi,probability_default_bureau,NULL as should_be_black_listed,should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckpassed_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_check_income_validator_calculation_method,credit_check_income_validator_contribution_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,evaluation_type,fraud_model_score,fraud_model_version,group_name,ia_overall_score,lbl,learning_population,ocurred_on,pd_calculation_method,policy,probability_default_addi,probability_default_bureau,should_be_black_listed,should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM union_bronze 
    
)   



, final AS (
    SELECT 
        *,
        date(ocurred_on ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM union_all_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_underwriting_fraud_stage_co_logs
country: co
silver_table_name: f_underwriting_fraud_stage_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'bypassed_reason', 'client_id', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_check_income_validator_calculation_method', 'credit_check_income_validator_contribution_type', 'credit_policy_name', 'credit_score', 'credit_score_name', 'credit_score_product', 'credit_status', 'credit_status_reason', 'evaluation_type', 'event_id', 'fraud_model_score', 'fraud_model_version', 'group_name', 'ia_overall_score', 'lbl', 'learning_population', 'ocurred_on', 'pd_calculation_method', 'policy', 'probability_default_addi', 'probability_default_bureau', 'should_be_black_listed', 'should_skip_idv', 'store_fraud_risk_level', 'tdsr']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ConditionalCreditCheckPassedCO': {'stage': 'underwriting_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'bypassed_reason', 'should_skip_idv'], 'custom_attributes': {}}, 'CreditCheckPassedCO': {'stage': 'underwriting_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'lbl', 'bypassed_reason'], 'custom_attributes': {}}, 'CreditCheckFailedCO': {'stage': 'underwriting_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'lbl', 'bypassed_reason'], 'custom_attributes': {}}, 'PreApprovalCreditCheckPassedCO': {'stage': 'underwriting_preapproval_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason'], 'custom_attributes': {}}, 'PreApprovalConditionalCreditCheckPassedCO': {'stage': 'underwriting_preapproval_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'lbl', 'bypassed_reason'], 'custom_attributes': {}}, 'PreApprovalCreditCheckFailedCO': {'stage': 'underwriting_preapproval_co', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population'], 'custom_attributes': {}}, 'ClientCreditCheckFailedPagoCO': {'stage': 'underwriting_rc_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'evaluation_type', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population'], 'custom_attributes': {}}, 'ClientCreditCheckPassedCO': {'stage': 'underwriting_rc_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'lbl', 'bypassed_reason'], 'custom_attributes': {}}, 'ClientCreditCheckFailedCO': {'stage': 'underwriting_rc_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'lbl', 'bypassed_reason'], 'custom_attributes': {}}, 'ClientCreditCheckPassedPagoCO': {'stage': 'underwriting_rc_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'evaluation_type', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'lbl'], 'custom_attributes': {}}, 'CreditCheckApprovedSantanderCO': {'stage': 'risk_evaluation_santander_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'group_name', 'client_max_exposure', 'tdsr', 'learning_population'], 'custom_attributes': {}}, 'PsychometricEvaluationIsRequiredCO': {'stage': 'underwriting_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score'], 'custom_attributes': {}}, 'PreApprovalConditionalCreditCheckPassedPagoCO': {'stage': 'underwriting_preapproval_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name'], 'custom_attributes': {}}, 'PreApprovalCreditCheckFailedPagoCO': {'stage': 'underwriting_preapproval_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population'], 'custom_attributes': {}}, 'PreApprovalCreditCheckPassedPagoCO': {'stage': 'underwriting_preapproval_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population'], 'custom_attributes': {}}, 'PsychometricEvaluationApproved': {'stage': 'psychometric_assessment', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'bypassed_reason'], 'custom_attributes': {}}, 'ConditionalCreditCheckPsychometricPassedCO': {'stage': 'underwriting_psychometric_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'lbl'], 'custom_attributes': {}}, 'CreditCheckPsychometricPassedCO': {'stage': 'underwriting_psychometric_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'lbl'], 'custom_attributes': {}}, 'CreditCheckPsychometricFailedCO': {'stage': 'underwriting_psychometric_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'lbl'], 'custom_attributes': {}}, 'ConditionalCreditCheckPassedPagoCO': {'stage': 'underwriting_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score_name', 'evaluation_type', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'lbl'], 'custom_attributes': {}}, 'CreditCheckFailedPagoCO': {'stage': 'underwriting_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score_name', 'evaluation_type', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'lbl', 'policy', 'store_fraud_risk_level', 'fraud_model_score', 'fraud_model_version'], 'custom_attributes': {}}, 'CreditCheckPassedPagoCO': {'stage': 'underwriting_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'should_be_black_listed', 'credit_score_name', 'evaluation_type', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'policy', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score'], 'custom_attributes': {}}, 'PsychometricEvaluationIsRequiredPagoCO': {'stage': 'underwriting_pago_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population'], 'custom_attributes': {}}, 'LoanAcceptedCO': {'stage': 'loan_acceptance_co', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population'], 'custom_attributes': {}}, 'FraudCheckFailedCO': {'stage': 'fraud_check_co', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'fraud_model_score', 'fraud_model_version'], 'custom_attributes': {}}, 'FraudCheckPassedCO': {'stage': 'fraud_check_co', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'fraud_model_score', 'fraud_model_version'], 'custom_attributes': {}}, 'CreditCheckFailed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_check_income_validator_calculation_method', 'credit_check_income_validator_contribution_type', 'tdsr', 'credit_policy_name', 'learning_population'], 'custom_attributes': {}}, 'ConditionalCreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_check_income_validator_calculation_method', 'credit_check_income_validator_contribution_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason'], 'custom_attributes': {}}, 'CreditCheckPassedWithLowerApprovedAmount': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_check_income_validator_calculation_method', 'credit_check_income_validator_contribution_type', 'pd_calculation_method', 'credit_policy_name', 'learning_population'], 'custom_attributes': {}}, 'ConditionalCreditCheckPassedWithLowerApprovedAmount': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_check_income_validator_calculation_method', 'credit_check_income_validator_contribution_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'should_skip_idv'], 'custom_attributes': {}}, 'ClientCreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_check_income_validator_calculation_method', 'credit_check_income_validator_contribution_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name'], 'custom_attributes': {}}, 'ClientCreditCheckPassedWithLowerApprovedAmount': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_check_income_validator_calculation_method', 'credit_check_income_validator_contribution_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name'], 'custom_attributes': {}}, 'ClientCreditCheckPassedWithInsufficientAddiCupo': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr'], 'custom_attributes': {}}, 'ClientCreditCheckFailed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name'], 'custom_attributes': {}}, 'ProspectUpgradedToClient': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'client_max_exposure', 'ocurred_on'], 'custom_attributes': {}}, 'CreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'bypassed_reason', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_check_income_validator_calculation_method', 'credit_check_income_validator_contribution_type', 'credit_policy_name', 'credit_score', 'credit_score_name', 'credit_score_product', 'credit_status', 'credit_status_reason', 'fraud_model_score', 'fraud_model_version', 'group_name', 'learning_population', 'lbl', 'ocurred_on', 'pd_calculation_method', 'probability_default_addi', 'probability_default_bureau', 'client_id', 'should_skip_idv', 'store_fraud_risk_level', 'tdsr'], 'custom_attributes': {}}}
events_keys: ['ConditionalCreditCheckPassedCO', 'CreditCheckPassedCO', 'CreditCheckFailedCO', 'PreApprovalCreditCheckPassedCO', 'PreApprovalConditionalCreditCheckPassedCO', 'PreApprovalCreditCheckFailedCO', 'ClientCreditCheckFailedPagoCO', 'ClientCreditCheckPassedCO', 'ClientCreditCheckFailedCO', 'ClientCreditCheckPassedPagoCO', 'CreditCheckApprovedSantanderCO', 'PsychometricEvaluationIsRequiredCO', 'PreApprovalConditionalCreditCheckPassedPagoCO', 'PreApprovalCreditCheckFailedPagoCO', 'PreApprovalCreditCheckPassedPagoCO', 'PsychometricEvaluationApproved', 'ConditionalCreditCheckPsychometricPassedCO', 'CreditCheckPsychometricPassedCO', 'CreditCheckPsychometricFailedCO', 'ConditionalCreditCheckPassedPagoCO', 'CreditCheckFailedPagoCO', 'CreditCheckPassedPagoCO', 'PsychometricEvaluationIsRequiredPagoCO', 'LoanAcceptedCO', 'FraudCheckFailedCO', 'FraudCheckPassedCO', 'CreditCheckFailed', 'ConditionalCreditCheckPassed', 'CreditCheckPassedWithLowerApprovedAmount', 'ConditionalCreditCheckPassedWithLowerApprovedAmount', 'ClientCreditCheckPassed', 'ClientCreditCheckPassedWithLowerApprovedAmount', 'ClientCreditCheckPassedWithInsufficientAddiCupo', 'ClientCreditCheckFailed', 'ProspectUpgradedToClient', 'CreditCheckPassed']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
