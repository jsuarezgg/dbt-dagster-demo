
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
conditionalcreditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedwithlowerapprovedamountbr_br AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedwithlowerapprovedamountbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedwithlowerapprovedamountbr_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassedwithlowerapprovedamountbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedbr_br AS ( 
    SELECT *
    FROM bronze.creditcheckfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedbr_br AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredbr_br AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalconditionalcreditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalconditionalcreditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckfailedbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanacceptedbybankinglicensepartnerbr_br AS ( 
    SELECT *
    FROM bronze.loanacceptedbybankinglicensepartnerbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,fraudcheckfailedbr_br AS ( 
    SELECT *
    FROM bronze.fraudcheckfailedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,fraudcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.fraudcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientfraudcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.clientfraudcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassed_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassed_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailed_br AS ( 
    SELECT *
    FROM bronze.creditcheckfailed_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassed_br AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassed_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassed_br AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassed_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectupgradedtoclient_br AS ( 
    SELECT *
    FROM bronze.prospectupgradedtoclient_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtoobtaincommercialinformationbr_br AS ( 
    SELECT *
    FROM bronze.failedtoobtaincommercialinformationbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,failedtoobtainscoringbr_br AS ( 
    SELECT *
    FROM bronze.failedtoobtainscoringbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalselectedbr_br AS ( 
    SELECT *
    FROM bronze.loanproposalselectedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalswithinvalidusuryratesbr_br AS ( 
    SELECT *
    FROM bronze.loanproposalswithinvalidusuryratesbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassedwithlowerapprovedamountbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassedwithlowerapprovedamountbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckfailedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,NULL as client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,NULL as credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,NULL as fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckfailedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM psychometricevaluationisrequiredbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,NULL as credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalconditionalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,NULL as credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalcreditcheckfailedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanacceptedbybankinglicensepartnerbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM fraudcheckfailedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM fraudcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,NULL as credit_status_reason,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientfraudcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassed_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckfailed_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,NULL as pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassed_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassed_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectupgradedtoclient_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM failedtoobtaincommercialinformationbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM failedtoobtainscoringbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanproposalselectedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanproposalswithinvalidusuryratesbr_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,last_event_ocurred_on_processed as ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_underwriting_fraud_stage_br  
    WHERE 
    silver.f_underwriting_fraud_stage_br.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN bypassed_reason is not null then struct(ocurred_on, bypassed_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).bypassed_reason as bypassed_reason,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN client_max_exposure is not null then struct(ocurred_on, client_max_exposure) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_max_exposure as client_max_exposure,
    element_at(array_sort(array_agg(CASE WHEN credit_check_income_net_value is not null then struct(ocurred_on, credit_check_income_net_value) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_check_income_net_value as credit_check_income_net_value,
    element_at(array_sort(array_agg(CASE WHEN credit_check_income_provider is not null then struct(ocurred_on, credit_check_income_provider) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_check_income_provider as credit_check_income_provider,
    element_at(array_sort(array_agg(CASE WHEN credit_check_income_type is not null then struct(ocurred_on, credit_check_income_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_check_income_type as credit_check_income_type,
    element_at(array_sort(array_agg(CASE WHEN credit_policy_name is not null then struct(ocurred_on, credit_policy_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_policy_name as credit_policy_name,
    element_at(array_sort(array_agg(CASE WHEN credit_score is not null then struct(ocurred_on, credit_score) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_score as credit_score,
    element_at(array_sort(array_agg(CASE WHEN credit_score_name is not null then struct(ocurred_on, credit_score_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_score_name as credit_score_name,
    element_at(array_sort(array_agg(CASE WHEN credit_score_product is not null then struct(ocurred_on, credit_score_product) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_score_product as credit_score_product,
    element_at(array_sort(array_agg(CASE WHEN credit_status is not null then struct(ocurred_on, credit_status) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_status as credit_status,
    element_at(array_sort(array_agg(CASE WHEN credit_status_reason is not null then struct(ocurred_on, credit_status_reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).credit_status_reason as credit_status_reason,
    element_at(array_sort(array_agg(CASE WHEN fraud_model_score is not null then struct(ocurred_on, fraud_model_score) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).fraud_model_score as fraud_model_score,
    element_at(array_sort(array_agg(CASE WHEN fraud_model_version is not null then struct(ocurred_on, fraud_model_version) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).fraud_model_version as fraud_model_version,
    element_at(array_sort(array_agg(CASE WHEN group_name is not null then struct(ocurred_on, group_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).group_name as group_name,
    element_at(array_sort(array_agg(CASE WHEN ia_overall_score is not null then struct(ocurred_on, ia_overall_score) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ia_overall_score as ia_overall_score,
    element_at(array_sort(array_agg(CASE WHEN learning_population is not null then struct(ocurred_on, learning_population) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).learning_population as learning_population,
    element_at(array_sort(array_agg(CASE WHEN pd_calculation_method is not null then struct(ocurred_on, pd_calculation_method) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).pd_calculation_method as pd_calculation_method,
    element_at(array_sort(array_agg(CASE WHEN probability_default_addi is not null then struct(ocurred_on, probability_default_addi) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).probability_default_addi as probability_default_addi,
    element_at(array_sort(array_agg(CASE WHEN probability_default_bureau is not null then struct(ocurred_on, probability_default_bureau) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).probability_default_bureau as probability_default_bureau,
    element_at(array_sort(array_agg(CASE WHEN should_skip_idv is not null then struct(ocurred_on, should_skip_idv) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).should_skip_idv as should_skip_idv,
    element_at(array_sort(array_agg(CASE WHEN store_fraud_risk_level is not null then struct(ocurred_on, store_fraud_risk_level) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_fraud_risk_level as store_fraud_risk_level,
    element_at(array_sort(array_agg(CASE WHEN tdsr is not null then struct(ocurred_on, tdsr) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).tdsr as tdsr,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    application_id
                       
           )


, final AS (
    SELECT 
        *,
        date(last_event_ocurred_on_processed ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM grouped_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_underwriting_fraud_stage_br
country: br
silver_table_name: f_underwriting_fraud_stage_br
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['application_id', 'bypassed_reason', 'client_id', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_policy_name', 'credit_score', 'credit_score_name', 'credit_score_product', 'credit_status', 'credit_status_reason', 'fraud_model_score', 'fraud_model_version', 'group_name', 'ia_overall_score', 'learning_population', 'ocurred_on', 'pd_calculation_method', 'probability_default_addi', 'probability_default_bureau', 'should_skip_idv', 'store_fraud_risk_level', 'tdsr']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ConditionalCreditCheckPassedBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'bypassed_reason', 'should_skip_idv', 'credit_score'], 'custom_attributes': {}}, 'ConditionalCreditCheckPassedWithLowerApprovedAmountBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'credit_score'], 'custom_attributes': {}}, 'CreditCheckPassedBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'bypassed_reason', 'should_skip_idv', 'credit_score'], 'custom_attributes': {}}, 'CreditCheckPassedWithLowerApprovedAmountBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'credit_score'], 'custom_attributes': {}}, 'CreditCheckFailedBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'bypassed_reason', 'credit_score'], 'custom_attributes': {}}, 'ClientCreditCheckPassedBR': {'stage': 'underwriting_rc_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'credit_score'], 'custom_attributes': {}}, 'ClientCreditCheckFailedBR': {'stage': 'underwriting_rc_br', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'credit_status', 'credit_status_reason', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'fraud_model_score', 'learning_population', 'bypassed_reason', 'group_name', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'credit_score'], 'custom_attributes': {}}, 'PsychometricEvaluationIsRequiredBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'credit_score'], 'custom_attributes': {}}, 'PreApprovalConditionalCreditCheckPassedBR': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_product', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'credit_policy_name', 'learning_population', 'credit_score'], 'custom_attributes': {}}, 'PreApprovalCreditCheckPassedBR': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'credit_score'], 'custom_attributes': {}}, 'PreApprovalCreditCheckFailedBR': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_product', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'credit_policy_name', 'learning_population', 'credit_score'], 'custom_attributes': {}}, 'LoanAcceptedByBankingLicensePartnerBR': {'stage': 'loan_acceptance_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_score'], 'custom_attributes': {}}, 'FraudCheckFailedBR': {'stage': 'fraud_check_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'fraud_model_score', 'fraud_model_version', 'credit_policy_name'], 'custom_attributes': {}}, 'FraudCheckPassedBR': {'stage': 'fraud_check_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'fraud_model_score', 'fraud_model_version', 'credit_policy_name'], 'custom_attributes': {}}, 'ClientFraudCheckPassedBR': {'stage': 'fraud_check_rc_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'fraud_model_score', 'fraud_model_version', 'credit_policy_name'], 'custom_attributes': {}}, 'CreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'should_skip_idv', 'credit_score'], 'custom_attributes': {}}, 'CreditCheckFailed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'credit_score'], 'custom_attributes': {}}, 'ConditionalCreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'should_skip_idv', 'credit_score'], 'custom_attributes': {}}, 'ClientCreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'credit_score'], 'custom_attributes': {}}, 'ProspectUpgradedToClient': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'FailedToObtainCommercialInformationBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id'], 'custom_attributes': {}}, 'FailedToObtainScoringBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id'], 'custom_attributes': {}}, 'LoanProposalSelectedBR': {'stage': 'loan_proposals_br', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id'], 'custom_attributes': {}}, 'LoanProposalsWithInvalidUsuryRatesBR': {'stage': 'underwriting_br', 'direct_attributes': ['application_id', 'client_id', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ConditionalCreditCheckPassedBR', 'ConditionalCreditCheckPassedWithLowerApprovedAmountBR', 'CreditCheckPassedBR', 'CreditCheckPassedWithLowerApprovedAmountBR', 'CreditCheckFailedBR', 'ClientCreditCheckPassedBR', 'ClientCreditCheckFailedBR', 'PsychometricEvaluationIsRequiredBR', 'PreApprovalConditionalCreditCheckPassedBR', 'PreApprovalCreditCheckPassedBR', 'PreApprovalCreditCheckFailedBR', 'LoanAcceptedByBankingLicensePartnerBR', 'FraudCheckFailedBR', 'FraudCheckPassedBR', 'ClientFraudCheckPassedBR', 'CreditCheckPassed', 'CreditCheckFailed', 'ConditionalCreditCheckPassed', 'ClientCreditCheckPassed', 'ProspectUpgradedToClient', 'FailedToObtainCommercialInformationBR', 'FailedToObtainScoringBR', 'LoanProposalSelectedBR', 'LoanProposalsWithInvalidUsuryRatesBR']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
