
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
identityphotosapproved_br AS ( 
    SELECT *
    FROM bronze.identityphotosapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosagentassigned_br AS ( 
    SELECT *
    FROM bronze.identityphotosagentassigned_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotoscollected_br AS ( 
    SELECT *
    FROM bronze.identityphotoscollected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosevaluationstarted_br AS ( 
    SELECT *
    FROM bronze.identityphotosevaluationstarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotoskeptbyagent_br AS ( 
    SELECT *
    FROM bronze.identityphotoskeptbyagent_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosstarted_br AS ( 
    SELECT *
    FROM bronze.identityphotosstarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosdiscarded_br AS ( 
    SELECT *
    FROM bronze.identityphotosdiscarded_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosrejected_br AS ( 
    SELECT *
    FROM bronze.identityphotosrejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosdiscardedbyrisk_br AS ( 
    SELECT *
    FROM bronze.identityphotosdiscardedbyrisk_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywastarted_br AS ( 
    SELECT *
    FROM bronze.identitywastarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywaapproved_br AS ( 
    SELECT *
    FROM bronze.identitywaapproved_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywadiscarded_br AS ( 
    SELECT *
    FROM bronze.identitywadiscarded_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywarejected_br AS ( 
    SELECT *
    FROM bronze.identitywarejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywainitialmessageresponsereceived_br AS ( 
    SELECT *
    FROM bronze.identitywainitialmessageresponsereceived_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywainputinformationcompleted_br AS ( 
    SELECT *
    FROM bronze.identitywainputinformationcompleted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywadiscardedbyrisk_br AS ( 
    SELECT *
    FROM bronze.identitywadiscardedbyrisk_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywaagentassigned_br AS ( 
    SELECT *
    FROM bronze.identitywaagentassigned_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywakeptbyagent_br AS ( 
    SELECT *
    FROM bronze.identitywakeptbyagent_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationrejected_br AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationrejected_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationsenttocontingency_br AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationsenttocontingency_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationstarted_br AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationstarted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosapproved_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosagentassigned_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotoscollected_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosevaluationstarted_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotoskeptbyagent_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosstarted_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosdiscarded_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosrejected_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosdiscardedbyrisk_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as reason,NULL as used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywastarted_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywaapproved_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywadiscarded_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywarejected_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywainitialmessageresponsereceived_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywainputinformationcompleted_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywadiscardedbyrisk_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywaagentassigned_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywakeptbyagent_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationrejected_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationrejected_at,prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationsenttocontingency_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,idv_provider,ip_address,NULL as is_high_risk,is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,prospectidentityverificationstarted_at,NULL as reason,NULL as used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationstarted_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    agent_user_id,application_id,client_id,identityphotosagentassigned_at,identityphotosapproved_at,identityphotoscollected_at,identityphotosdiscarded_at,identityphotosdiscardedbyrisk_at,identityphotosevaluationstarted_at,identityphotoskeptbyagent_at,identityphotosrejected_at,identityphotosstarted_at,identitywaagentassigned_at,identitywaapproved_at,identitywadiscarded_at,identitywadiscardedbyrisk_at,identitywainitialmessageresponsereceived_at,identitywainputinformationcompleted_at,identitywakeptbyagent_at,identitywarejected_at,identitywastarted_at,idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,prospectidentityverificationrejected_at,prospectidentityverificationsenttocontingency_at,prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    agent_user_id,application_id,client_id,identityphotosagentassigned_at,identityphotosapproved_at,identityphotoscollected_at,identityphotosdiscarded_at,identityphotosdiscardedbyrisk_at,identityphotosevaluationstarted_at,identityphotoskeptbyagent_at,identityphotosrejected_at,identityphotosstarted_at,identitywaagentassigned_at,identitywaapproved_at,identitywadiscarded_at,identitywadiscardedbyrisk_at,identitywainitialmessageresponsereceived_at,identitywainputinformationcompleted_at,identitywakeptbyagent_at,identitywarejected_at,identitywastarted_at,idv_provider,ip_address,is_high_risk,is_mobile,observations,last_event_ocurred_on_processed as ocurred_on,prospectidentityverificationrejected_at,prospectidentityverificationsenttocontingency_at,prospectidentityverificationstarted_at,reason,used_policy_id,user_agent,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_idv_stage_br  
    WHERE 
    silver.f_idv_stage_br.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN agent_user_id is not null then struct(ocurred_on, agent_user_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).agent_user_id as agent_user_id,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN identityphotosagentassigned_at is not null then struct(ocurred_on, identityphotosagentassigned_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosagentassigned_at as identityphotosagentassigned_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosapproved_at is not null then struct(ocurred_on, identityphotosapproved_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosapproved_at as identityphotosapproved_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotoscollected_at is not null then struct(ocurred_on, identityphotoscollected_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotoscollected_at as identityphotoscollected_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosdiscarded_at is not null then struct(ocurred_on, identityphotosdiscarded_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosdiscarded_at as identityphotosdiscarded_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosdiscardedbyrisk_at is not null then struct(ocurred_on, identityphotosdiscardedbyrisk_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosdiscardedbyrisk_at as identityphotosdiscardedbyrisk_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosevaluationstarted_at is not null then struct(ocurred_on, identityphotosevaluationstarted_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosevaluationstarted_at as identityphotosevaluationstarted_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotoskeptbyagent_at is not null then struct(ocurred_on, identityphotoskeptbyagent_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotoskeptbyagent_at as identityphotoskeptbyagent_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosrejected_at is not null then struct(ocurred_on, identityphotosrejected_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosrejected_at as identityphotosrejected_at,
    element_at(array_sort(array_agg(CASE WHEN identityphotosstarted_at is not null then struct(ocurred_on, identityphotosstarted_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identityphotosstarted_at as identityphotosstarted_at,
    element_at(array_sort(array_agg(CASE WHEN identitywaagentassigned_at is not null then struct(ocurred_on, identitywaagentassigned_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywaagentassigned_at as identitywaagentassigned_at,
    element_at(array_sort(array_agg(CASE WHEN identitywaapproved_at is not null then struct(ocurred_on, identitywaapproved_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywaapproved_at as identitywaapproved_at,
    element_at(array_sort(array_agg(CASE WHEN identitywadiscarded_at is not null then struct(ocurred_on, identitywadiscarded_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywadiscarded_at as identitywadiscarded_at,
    element_at(array_sort(array_agg(CASE WHEN identitywadiscardedbyrisk_at is not null then struct(ocurred_on, identitywadiscardedbyrisk_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywadiscardedbyrisk_at as identitywadiscardedbyrisk_at,
    element_at(array_sort(array_agg(CASE WHEN identitywainitialmessageresponsereceived_at is not null then struct(ocurred_on, identitywainitialmessageresponsereceived_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywainitialmessageresponsereceived_at as identitywainitialmessageresponsereceived_at,
    element_at(array_sort(array_agg(CASE WHEN identitywainputinformationcompleted_at is not null then struct(ocurred_on, identitywainputinformationcompleted_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywainputinformationcompleted_at as identitywainputinformationcompleted_at,
    element_at(array_sort(array_agg(CASE WHEN identitywakeptbyagent_at is not null then struct(ocurred_on, identitywakeptbyagent_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywakeptbyagent_at as identitywakeptbyagent_at,
    element_at(array_sort(array_agg(CASE WHEN identitywarejected_at is not null then struct(ocurred_on, identitywarejected_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywarejected_at as identitywarejected_at,
    element_at(array_sort(array_agg(CASE WHEN identitywastarted_at is not null then struct(ocurred_on, identitywastarted_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).identitywastarted_at as identitywastarted_at,
    element_at(array_sort(array_agg(CASE WHEN idv_provider is not null then struct(ocurred_on, idv_provider) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).idv_provider as idv_provider,
    element_at(array_sort(array_agg(CASE WHEN ip_address is not null then struct(ocurred_on, ip_address) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ip_address as ip_address,
    element_at(array_sort(array_agg(CASE WHEN is_high_risk is not null then struct(ocurred_on, is_high_risk) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).is_high_risk as is_high_risk,
    element_at(array_sort(array_agg(CASE WHEN is_mobile is not null then struct(ocurred_on, is_mobile) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).is_mobile as is_mobile,
    element_at(array_sort(array_agg(CASE WHEN observations is not null then struct(ocurred_on, observations) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).observations as observations,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationrejected_at is not null then struct(ocurred_on, prospectidentityverificationrejected_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationrejected_at as prospectidentityverificationrejected_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationsenttocontingency_at is not null then struct(ocurred_on, prospectidentityverificationsenttocontingency_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationsenttocontingency_at as prospectidentityverificationsenttocontingency_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationstarted_at is not null then struct(ocurred_on, prospectidentityverificationstarted_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationstarted_at as prospectidentityverificationstarted_at,
    element_at(array_sort(array_agg(CASE WHEN reason is not null then struct(ocurred_on, reason) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).reason as reason,
    element_at(array_sort(array_agg(CASE WHEN used_policy_id is not null then struct(ocurred_on, used_policy_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).used_policy_id as used_policy_id,
    element_at(array_sort(array_agg(CASE WHEN user_agent is not null then struct(ocurred_on, user_agent) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).user_agent as user_agent,
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
this: silver.f_idv_stage_br
country: br
silver_table_name: f_idv_stage_br
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['agent_user_id', 'application_id', 'client_id', 'identityphotosagentassigned_at', 'identityphotosapproved_at', 'identityphotoscollected_at', 'identityphotosdiscarded_at', 'identityphotosdiscardedbyrisk_at', 'identityphotosevaluationstarted_at', 'identityphotoskeptbyagent_at', 'identityphotosrejected_at', 'identityphotosstarted_at', 'identitywaagentassigned_at', 'identitywaapproved_at', 'identitywadiscarded_at', 'identitywadiscardedbyrisk_at', 'identitywainitialmessageresponsereceived_at', 'identitywainputinformationcompleted_at', 'identitywakeptbyagent_at', 'identitywarejected_at', 'identitywastarted_at', 'idv_provider', 'ip_address', 'is_high_risk', 'is_mobile', 'observations', 'ocurred_on', 'prospectidentityverificationrejected_at', 'prospectidentityverificationsenttocontingency_at', 'prospectidentityverificationstarted_at', 'reason', 'used_policy_id', 'user_agent']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'IdentityPhotosApproved': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosapproved_at']}, 'IdentityPhotosAgentAssigned': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosagentassigned_at']}, 'IdentityPhotosCollected': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotoscollected_at']}, 'IdentityPhotosEvaluationStarted': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosevaluationstarted_at']}, 'IdentityPhotosKeptByAgent': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotoskeptbyagent_at']}, 'IdentityPhotosStarted': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosstarted_at']}, 'IdentityPhotosDiscarded': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosdiscarded_at']}, 'IdentityPhotosRejected': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosrejected_at']}, 'IdentityPhotosDiscardedByRisk': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosdiscardedbyrisk_at']}, 'IdentityWAStarted': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'identitywastarted_at']}, 'IdentityWAApproved': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'identitywaapproved_at']}, 'IdentityWADiscarded': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'identitywadiscarded_at']}, 'IdentityWARejected': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'identitywarejected_at']}, 'IdentityWAInitialMessageResponseReceived': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'identitywainitialmessageresponsereceived_at']}, 'IdentityWAInputInformationCompleted': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'identitywainputinformationcompleted_at']}, 'IdentityWADiscardedByRisk': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'identitywadiscardedbyrisk_at']}, 'IdentityWAAgentAssigned': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'idv_provider', 'identitywaagentassigned_at']}, 'IdentityWAKeptByAgent': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'identitywakeptbyagent_at'], 'custom_attributes': {}}, 'ProspectIdentityVerificationRejected': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationrejected_at']}, 'ProspectIdentityVerificationSentToContingency': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationsenttocontingency_at']}, 'ProspectIdentityVerificationStarted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'ip_address', 'user_agent', 'is_mobile', 'idv_provider', 'prospectidentityverificationstarted_at']}}
events_keys: ['IdentityPhotosApproved', 'IdentityPhotosAgentAssigned', 'IdentityPhotosCollected', 'IdentityPhotosEvaluationStarted', 'IdentityPhotosKeptByAgent', 'IdentityPhotosStarted', 'IdentityPhotosDiscarded', 'IdentityPhotosRejected', 'IdentityPhotosDiscardedByRisk', 'IdentityWAStarted', 'IdentityWAApproved', 'IdentityWADiscarded', 'IdentityWARejected', 'IdentityWAInitialMessageResponseReceived', 'IdentityWAInputInformationCompleted', 'IdentityWADiscardedByRisk', 'IdentityWAAgentAssigned', 'IdentityWAKeptByAgent', 'ProspectIdentityVerificationRejected', 'ProspectIdentityVerificationSentToContingency', 'ProspectIdentityVerificationStarted']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
