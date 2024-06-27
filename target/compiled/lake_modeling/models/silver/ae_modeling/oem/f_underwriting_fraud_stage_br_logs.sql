
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
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM conditionalcreditcheckpassedwithlowerapprovedamountbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckpassedwithlowerapprovedamountbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckfailedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,NULL as client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,NULL as credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,NULL as fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckfailedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM psychometricevaluationisrequiredbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,NULL as credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalconditionalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalcreditcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,NULL as credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM preapprovalcreditcheckfailedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM loanacceptedbybankinglicensepartnerbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM fraudcheckfailedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM fraudcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,credit_status,NULL as credit_status_reason,fraud_model_score,fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM clientfraudcheckpassedbr_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckpassed_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM creditcheckfailed_br
    UNION ALL
    SELECT 
        application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,learning_population,ocurred_on,NULL as pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM conditionalcreditcheckpassed_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,NULL as should_skip_idv,store_fraud_risk_level,tdsr,
    event_name,
    event_id
    FROM clientcreditcheckpassed_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM prospectupgradedtoclient_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM failedtoobtaincommercialinformationbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM failedtoobtainscoringbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM loanproposalselectedbr_br
    UNION ALL
    SELECT 
        application_id,NULL as bypassed_reason,client_id,NULL as client_max_exposure,NULL as credit_check_income_net_value,NULL as credit_check_income_provider,NULL as credit_check_income_type,NULL as credit_policy_name,NULL as credit_score,NULL as credit_score_name,NULL as credit_score_product,NULL as credit_status,NULL as credit_status_reason,NULL as fraud_model_score,NULL as fraud_model_version,NULL as group_name,NULL as ia_overall_score,NULL as learning_population,ocurred_on,NULL as pd_calculation_method,NULL as probability_default_addi,NULL as probability_default_bureau,NULL as should_skip_idv,NULL as store_fraud_risk_level,NULL as tdsr,
    event_name,
    event_id
    FROM loanproposalswithinvalidusuryratesbr_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    application_id,bypassed_reason,client_id,client_max_exposure,credit_check_income_net_value,credit_check_income_provider,credit_check_income_type,credit_policy_name,credit_score,credit_score_name,credit_score_product,credit_status,credit_status_reason,fraud_model_score,fraud_model_version,group_name,ia_overall_score,learning_population,ocurred_on,pd_calculation_method,probability_default_addi,probability_default_bureau,should_skip_idv,store_fraud_risk_level,tdsr,
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
this: silver.f_underwriting_fraud_stage_br_logs
country: br
silver_table_name: f_underwriting_fraud_stage_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['application_id', 'bypassed_reason', 'client_id', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'credit_policy_name', 'credit_score', 'credit_score_name', 'credit_score_product', 'credit_status', 'credit_status_reason', 'event_id', 'fraud_model_score', 'fraud_model_version', 'group_name', 'ia_overall_score', 'learning_population', 'ocurred_on', 'pd_calculation_method', 'probability_default_addi', 'probability_default_bureau', 'should_skip_idv', 'store_fraud_risk_level', 'tdsr']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'ConditionalCreditCheckPassedBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'bypassed_reason', 'should_skip_idv', 'credit_score'], 'custom_attributes': {}}, 'ConditionalCreditCheckPassedWithLowerApprovedAmountBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'credit_score'], 'custom_attributes': {}}, 'CreditCheckPassedBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'bypassed_reason', 'should_skip_idv', 'credit_score'], 'custom_attributes': {}}, 'CreditCheckPassedWithLowerApprovedAmountBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'credit_score'], 'custom_attributes': {}}, 'CreditCheckFailedBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'ia_overall_score', 'bypassed_reason', 'credit_score'], 'custom_attributes': {}}, 'ClientCreditCheckPassedBR': {'stage': 'underwriting_rc_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'credit_score'], 'custom_attributes': {}}, 'ClientCreditCheckFailedBR': {'stage': 'underwriting_rc_br', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'credit_status', 'credit_status_reason', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'fraud_model_score', 'learning_population', 'bypassed_reason', 'group_name', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'credit_score'], 'custom_attributes': {}}, 'PsychometricEvaluationIsRequiredBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'credit_score'], 'custom_attributes': {}}, 'PreApprovalConditionalCreditCheckPassedBR': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_product', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'credit_policy_name', 'learning_population', 'credit_score'], 'custom_attributes': {}}, 'PreApprovalCreditCheckPassedBR': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'credit_score'], 'custom_attributes': {}}, 'PreApprovalCreditCheckFailedBR': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_product', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'credit_policy_name', 'learning_population', 'credit_score'], 'custom_attributes': {}}, 'LoanAcceptedByBankingLicensePartnerBR': {'stage': 'loan_acceptance_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_score'], 'custom_attributes': {}}, 'FraudCheckFailedBR': {'stage': 'fraud_check_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'fraud_model_score', 'fraud_model_version', 'credit_policy_name'], 'custom_attributes': {}}, 'FraudCheckPassedBR': {'stage': 'fraud_check_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'fraud_model_score', 'fraud_model_version', 'credit_policy_name'], 'custom_attributes': {}}, 'ClientFraudCheckPassedBR': {'stage': 'fraud_check_rc_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'fraud_model_score', 'fraud_model_version', 'credit_policy_name'], 'custom_attributes': {}}, 'CreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'should_skip_idv', 'credit_score'], 'custom_attributes': {}}, 'CreditCheckFailed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'credit_score'], 'custom_attributes': {}}, 'ConditionalCreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'credit_policy_name', 'learning_population', 'bypassed_reason', 'should_skip_idv', 'credit_score'], 'custom_attributes': {}}, 'ClientCreditCheckPassed': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on', 'credit_status', 'credit_status_reason', 'credit_score_name', 'credit_score_product', 'probability_default_bureau', 'probability_default_addi', 'group_name', 'fraud_model_score', 'fraud_model_version', 'store_fraud_risk_level', 'client_max_exposure', 'credit_check_income_net_value', 'credit_check_income_provider', 'credit_check_income_type', 'tdsr', 'pd_calculation_method', 'credit_policy_name', 'credit_score'], 'custom_attributes': {}}, 'ProspectUpgradedToClient': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'FailedToObtainCommercialInformationBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'FailedToObtainScoringBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'LoanProposalSelectedBR': {'stage': 'loan_proposals_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on'], 'custom_attributes': {}}, 'LoanProposalsWithInvalidUsuryRatesBR': {'stage': 'underwriting_br', 'direct_attributes': ['event_id', 'application_id', 'client_id', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['ConditionalCreditCheckPassedBR', 'ConditionalCreditCheckPassedWithLowerApprovedAmountBR', 'CreditCheckPassedBR', 'CreditCheckPassedWithLowerApprovedAmountBR', 'CreditCheckFailedBR', 'ClientCreditCheckPassedBR', 'ClientCreditCheckFailedBR', 'PsychometricEvaluationIsRequiredBR', 'PreApprovalConditionalCreditCheckPassedBR', 'PreApprovalCreditCheckPassedBR', 'PreApprovalCreditCheckFailedBR', 'LoanAcceptedByBankingLicensePartnerBR', 'FraudCheckFailedBR', 'FraudCheckPassedBR', 'ClientFraudCheckPassedBR', 'CreditCheckPassed', 'CreditCheckFailed', 'ConditionalCreditCheckPassed', 'ClientCreditCheckPassed', 'ProspectUpgradedToClient', 'FailedToObtainCommercialInformationBR', 'FailedToObtainScoringBR', 'LoanProposalSelectedBR', 'LoanProposalsWithInvalidUsuryRatesBR']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
