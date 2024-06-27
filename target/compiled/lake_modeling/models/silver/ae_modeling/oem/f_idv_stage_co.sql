
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
identityphotosapproved_co AS ( 
    SELECT *
    FROM bronze.identityphotosapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosagentassigned_co AS ( 
    SELECT *
    FROM bronze.identityphotosagentassigned_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotoscollected_co AS ( 
    SELECT *
    FROM bronze.identityphotoscollected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosevaluationstarted_co AS ( 
    SELECT *
    FROM bronze.identityphotosevaluationstarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotoskeptbyagent_co AS ( 
    SELECT *
    FROM bronze.identityphotoskeptbyagent_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosstarted_co AS ( 
    SELECT *
    FROM bronze.identityphotosstarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosdiscarded_co AS ( 
    SELECT *
    FROM bronze.identityphotosdiscarded_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosrejected_co AS ( 
    SELECT *
    FROM bronze.identityphotosrejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identityphotosdiscardedbyrisk_co AS ( 
    SELECT *
    FROM bronze.identityphotosdiscardedbyrisk_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywastarted_co AS ( 
    SELECT *
    FROM bronze.identitywastarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywaapproved_co AS ( 
    SELECT *
    FROM bronze.identitywaapproved_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywadiscarded_co AS ( 
    SELECT *
    FROM bronze.identitywadiscarded_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywarejected_co AS ( 
    SELECT *
    FROM bronze.identitywarejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywainitialmessageresponsereceived_co AS ( 
    SELECT *
    FROM bronze.identitywainitialmessageresponsereceived_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywainputinformationcompleted_co AS ( 
    SELECT *
    FROM bronze.identitywainputinformationcompleted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywadiscardedbyrisk_co AS ( 
    SELECT *
    FROM bronze.identitywadiscardedbyrisk_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywaagentassigned_co AS ( 
    SELECT *
    FROM bronze.identitywaagentassigned_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,identitywakeptbyagent_co AS ( 
    SELECT *
    FROM bronze.identitywakeptbyagent_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationdiscardedbycreditrisk_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationdiscardedbycreditrisk_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationinitialmessagedelivered_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationinitialmessagedelivered_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationinitialmessageresponsearrived_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationinitialmessageresponsearrived_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationinitialmessageresponsereceived_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationinitialmessageresponsereceived_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationinitialmessagestatusupdated_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationinitialmessagestatusupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationinputinformationcompleted_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationinputinformationcompleted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationrejected_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationrejected_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationsenttocontingency_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationsenttocontingency_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationstarted_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationstarted_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectidentityverificationtaken_co AS ( 
    SELECT *
    FROM bronze.prospectidentityverificationtaken_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosapproved_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosagentassigned_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotoscollected_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosevaluationstarted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotoskeptbyagent_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosstarted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosdiscarded_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosrejected_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identityphotosdiscardedbyrisk_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,identitywastarted_at,NULL as idv_provider,ip_address,NULL as is_high_risk,is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywastarted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywaapproved_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywadiscarded_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywarejected_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywainitialmessageresponsereceived_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywainputinformationcompleted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywadiscardedbyrisk_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywaagentassigned_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM identitywakeptbyagent_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationdiscardedbycreditrisk_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationinitialmessagedelivered_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationinitialmessageresponsearrived_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationinitialmessageresponsereceived_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationinitialmessagestatusupdated_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationinputinformationcompleted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,reason,used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationrejected_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationsenttocontingency_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,idv_provider,ip_address,NULL as is_high_risk,is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,prospectidentityverificationstarted_at,NULL as prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationstarted_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as identityphotosagentassigned_at,NULL as identityphotosapproved_at,NULL as identityphotoscollected_at,NULL as identityphotosdiscarded_at,NULL as identityphotosdiscardedbyrisk_at,NULL as identityphotosevaluationstarted_at,NULL as identityphotoskeptbyagent_at,NULL as identityphotosrejected_at,NULL as identityphotosstarted_at,NULL as identitywaagentassigned_at,NULL as identitywaapproved_at,NULL as identitywadiscarded_at,NULL as identitywadiscardedbyrisk_at,NULL as identitywainitialmessageresponsereceived_at,NULL as identitywainputinformationcompleted_at,NULL as identitywakeptbyagent_at,NULL as identitywarejected_at,NULL as identitywastarted_at,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as prospectidentityverificationdiscardedbycreditrisk_at,NULL as prospectidentityverificationinitialmessagedelivered_at,NULL as prospectidentityverificationinitialmessageresponsearrived_at,NULL as prospectidentityverificationinitialmessageresponsereceived_at,NULL as prospectidentityverificationinitialmessagestatusupdated_at,NULL as prospectidentityverificationinputinformationcompleted_at,NULL as prospectidentityverificationrejected_at,NULL as prospectidentityverificationsenttocontingency_at,NULL as prospectidentityverificationstarted_at,prospectidentityverificationtaken_at,NULL as reason,NULL as used_policy_id,NULL as user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectidentityverificationtaken_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    agent_user_id,application_id,client_id,identityphotosagentassigned_at,identityphotosapproved_at,identityphotoscollected_at,identityphotosdiscarded_at,identityphotosdiscardedbyrisk_at,identityphotosevaluationstarted_at,identityphotoskeptbyagent_at,identityphotosrejected_at,identityphotosstarted_at,identitywaagentassigned_at,identitywaapproved_at,identitywadiscarded_at,identitywadiscardedbyrisk_at,identitywainitialmessageresponsereceived_at,identitywainputinformationcompleted_at,identitywakeptbyagent_at,identitywarejected_at,identitywastarted_at,idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,prospectidentityverificationdiscardedbycreditrisk_at,prospectidentityverificationinitialmessagedelivered_at,prospectidentityverificationinitialmessageresponsearrived_at,prospectidentityverificationinitialmessageresponsereceived_at,prospectidentityverificationinitialmessagestatusupdated_at,prospectidentityverificationinputinformationcompleted_at,prospectidentityverificationrejected_at,prospectidentityverificationsenttocontingency_at,prospectidentityverificationstarted_at,prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    agent_user_id,application_id,client_id,identityphotosagentassigned_at,identityphotosapproved_at,identityphotoscollected_at,identityphotosdiscarded_at,identityphotosdiscardedbyrisk_at,identityphotosevaluationstarted_at,identityphotoskeptbyagent_at,identityphotosrejected_at,identityphotosstarted_at,identitywaagentassigned_at,identitywaapproved_at,identitywadiscarded_at,identitywadiscardedbyrisk_at,identitywainitialmessageresponsereceived_at,identitywainputinformationcompleted_at,identitywakeptbyagent_at,identitywarejected_at,identitywastarted_at,idv_provider,ip_address,is_high_risk,is_mobile,observations,last_event_ocurred_on_processed as ocurred_on,prospectidentityverificationdiscardedbycreditrisk_at,prospectidentityverificationinitialmessagedelivered_at,prospectidentityverificationinitialmessageresponsearrived_at,prospectidentityverificationinitialmessageresponsereceived_at,prospectidentityverificationinitialmessagestatusupdated_at,prospectidentityverificationinputinformationcompleted_at,prospectidentityverificationrejected_at,prospectidentityverificationsenttocontingency_at,prospectidentityverificationstarted_at,prospectidentityverificationtaken_at,reason,used_policy_id,user_agent,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_idv_stage_co  
    WHERE 
    silver.f_idv_stage_co.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
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
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationdiscardedbycreditrisk_at is not null then struct(ocurred_on, prospectidentityverificationdiscardedbycreditrisk_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationdiscardedbycreditrisk_at as prospectidentityverificationdiscardedbycreditrisk_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationinitialmessagedelivered_at is not null then struct(ocurred_on, prospectidentityverificationinitialmessagedelivered_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationinitialmessagedelivered_at as prospectidentityverificationinitialmessagedelivered_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationinitialmessageresponsearrived_at is not null then struct(ocurred_on, prospectidentityverificationinitialmessageresponsearrived_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationinitialmessageresponsearrived_at as prospectidentityverificationinitialmessageresponsearrived_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationinitialmessageresponsereceived_at is not null then struct(ocurred_on, prospectidentityverificationinitialmessageresponsereceived_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationinitialmessageresponsereceived_at as prospectidentityverificationinitialmessageresponsereceived_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationinitialmessagestatusupdated_at is not null then struct(ocurred_on, prospectidentityverificationinitialmessagestatusupdated_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationinitialmessagestatusupdated_at as prospectidentityverificationinitialmessagestatusupdated_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationinputinformationcompleted_at is not null then struct(ocurred_on, prospectidentityverificationinputinformationcompleted_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationinputinformationcompleted_at as prospectidentityverificationinputinformationcompleted_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationrejected_at is not null then struct(ocurred_on, prospectidentityverificationrejected_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationrejected_at as prospectidentityverificationrejected_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationsenttocontingency_at is not null then struct(ocurred_on, prospectidentityverificationsenttocontingency_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationsenttocontingency_at as prospectidentityverificationsenttocontingency_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationstarted_at is not null then struct(ocurred_on, prospectidentityverificationstarted_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationstarted_at as prospectidentityverificationstarted_at,
    element_at(array_sort(array_agg(CASE WHEN prospectidentityverificationtaken_at is not null then struct(ocurred_on, prospectidentityverificationtaken_at) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).prospectidentityverificationtaken_at as prospectidentityverificationtaken_at,
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
this: silver.f_idv_stage_co
country: co
silver_table_name: f_idv_stage_co
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['agent_user_id', 'application_id', 'client_id', 'identityphotosagentassigned_at', 'identityphotosapproved_at', 'identityphotoscollected_at', 'identityphotosdiscarded_at', 'identityphotosdiscardedbyrisk_at', 'identityphotosevaluationstarted_at', 'identityphotoskeptbyagent_at', 'identityphotosrejected_at', 'identityphotosstarted_at', 'identitywaagentassigned_at', 'identitywaapproved_at', 'identitywadiscarded_at', 'identitywadiscardedbyrisk_at', 'identitywainitialmessageresponsereceived_at', 'identitywainputinformationcompleted_at', 'identitywakeptbyagent_at', 'identitywarejected_at', 'identitywastarted_at', 'idv_provider', 'ip_address', 'is_high_risk', 'is_mobile', 'observations', 'ocurred_on', 'prospectidentityverificationdiscardedbycreditrisk_at', 'prospectidentityverificationinitialmessagedelivered_at', 'prospectidentityverificationinitialmessageresponsearrived_at', 'prospectidentityverificationinitialmessageresponsereceived_at', 'prospectidentityverificationinitialmessagestatusupdated_at', 'prospectidentityverificationinputinformationcompleted_at', 'prospectidentityverificationrejected_at', 'prospectidentityverificationsenttocontingency_at', 'prospectidentityverificationstarted_at', 'prospectidentityverificationtaken_at', 'reason', 'used_policy_id', 'user_agent']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'IdentityPhotosApproved': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosapproved_at']}, 'IdentityPhotosAgentAssigned': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosagentassigned_at']}, 'IdentityPhotosCollected': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotoscollected_at']}, 'IdentityPhotosEvaluationStarted': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosevaluationstarted_at']}, 'IdentityPhotosKeptByAgent': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotoskeptbyagent_at']}, 'IdentityPhotosStarted': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosstarted_at']}, 'IdentityPhotosDiscarded': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosdiscarded_at']}, 'IdentityPhotosRejected': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosrejected_at']}, 'IdentityPhotosDiscardedByRisk': {'stage': 'identity_photos', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations', 'identityphotosdiscardedbyrisk_at']}, 'IdentityWAStarted': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'identitywastarted_at']}, 'IdentityWAApproved': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'identitywaapproved_at']}, 'IdentityWADiscarded': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'identitywadiscarded_at']}, 'IdentityWARejected': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'identitywarejected_at']}, 'IdentityWAInitialMessageResponseReceived': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'identitywainitialmessageresponsereceived_at']}, 'IdentityWAInputInformationCompleted': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'identitywainputinformationcompleted_at']}, 'IdentityWADiscardedByRisk': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'identitywadiscardedbyrisk_at']}, 'IdentityWAAgentAssigned': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'identitywaagentassigned_at']}, 'IdentityWAKeptByAgent': {'stage': 'identity_wa', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'identitywakeptbyagent_at'], 'custom_attributes': {}}, 'ProspectIdentityVerificationDiscardedByCreditRisk': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'prospectidentityverificationdiscardedbycreditrisk_at']}, 'ProspectIdentityVerificationInitialMessageDelivered': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationinitialmessagedelivered_at']}, 'ProspectIdentityVerificationInitialMessageResponseArrived': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationinitialmessageresponsearrived_at']}, 'ProspectIdentityVerificationInitialMessageResponseReceived': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationinitialmessageresponsereceived_at']}, 'ProspectIdentityVerificationInitialMessageStatusUpdated': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationinitialmessagestatusupdated_at']}, 'ProspectIdentityVerificationInputInformationCompleted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationinputinformationcompleted_at']}, 'ProspectIdentityVerificationRejected': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations', 'prospectidentityverificationrejected_at']}, 'ProspectIdentityVerificationSentToContingency': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationsenttocontingency_at']}, 'ProspectIdentityVerificationStarted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'ip_address', 'user_agent', 'is_mobile', 'idv_provider', 'prospectidentityverificationstarted_at']}, 'ProspectIdentityVerificationTaken': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['application_id', 'ocurred_on', 'client_id', 'prospectidentityverificationtaken_at']}}
events_keys: ['IdentityPhotosApproved', 'IdentityPhotosAgentAssigned', 'IdentityPhotosCollected', 'IdentityPhotosEvaluationStarted', 'IdentityPhotosKeptByAgent', 'IdentityPhotosStarted', 'IdentityPhotosDiscarded', 'IdentityPhotosRejected', 'IdentityPhotosDiscardedByRisk', 'IdentityWAStarted', 'IdentityWAApproved', 'IdentityWADiscarded', 'IdentityWARejected', 'IdentityWAInitialMessageResponseReceived', 'IdentityWAInputInformationCompleted', 'IdentityWADiscardedByRisk', 'IdentityWAAgentAssigned', 'IdentityWAKeptByAgent', 'ProspectIdentityVerificationDiscardedByCreditRisk', 'ProspectIdentityVerificationInitialMessageDelivered', 'ProspectIdentityVerificationInitialMessageResponseArrived', 'ProspectIdentityVerificationInitialMessageResponseReceived', 'ProspectIdentityVerificationInitialMessageStatusUpdated', 'ProspectIdentityVerificationInputInformationCompleted', 'ProspectIdentityVerificationRejected', 'ProspectIdentityVerificationSentToContingency', 'ProspectIdentityVerificationStarted', 'ProspectIdentityVerificationTaken']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
