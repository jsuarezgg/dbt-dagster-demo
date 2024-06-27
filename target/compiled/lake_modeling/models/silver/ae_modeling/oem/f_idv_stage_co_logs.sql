
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
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosapproved_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosagentassigned_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotoscollected_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosevaluationstarted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotoskeptbyagent_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosstarted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosdiscarded_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosrejected_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosdiscardedbyrisk_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,NULL as is_high_risk,is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,user_agent,
    event_name,
    event_id
    FROM identitywastarted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywaapproved_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywadiscarded_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywarejected_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywainitialmessageresponsereceived_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywainputinformationcompleted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywadiscardedbyrisk_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywaagentassigned_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywakeptbyagent_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationdiscardedbycreditrisk_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationinitialmessagedelivered_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationinitialmessageresponsearrived_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationinitialmessageresponsereceived_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationinitialmessagestatusupdated_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationinputinformationcompleted_co
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationrejected_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationsenttocontingency_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,idv_provider,ip_address,NULL as is_high_risk,is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationstarted_co
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationtaken_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    agent_user_id,application_id,client_id,idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
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
this: silver.f_idv_stage_co_logs
country: co
silver_table_name: f_idv_stage_co_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['agent_user_id', 'application_id', 'client_id', 'event_id', 'idv_provider', 'ip_address', 'is_high_risk', 'is_mobile', 'observations', 'ocurred_on', 'reason', 'used_policy_id', 'user_agent']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'IdentityPhotosApproved': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosAgentAssigned': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosCollected': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosEvaluationStarted': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosKeptByAgent': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosStarted': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosDiscarded': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosRejected': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosDiscardedByRisk': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityWAStarted': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile']}, 'IdentityWAApproved': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'IdentityWADiscarded': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'IdentityWARejected': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'IdentityWAInitialMessageResponseReceived': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'IdentityWAInputInformationCompleted': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'IdentityWADiscardedByRisk': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'IdentityWAAgentAssigned': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id']}, 'IdentityWAKeptByAgent': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id'], 'custom_attributes': {}}, 'ProspectIdentityVerificationDiscardedByCreditRisk': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'ProspectIdentityVerificationInitialMessageDelivered': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'ProspectIdentityVerificationInitialMessageResponseArrived': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'ProspectIdentityVerificationInitialMessageResponseReceived': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'ProspectIdentityVerificationInitialMessageStatusUpdated': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'ProspectIdentityVerificationInputInformationCompleted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'ProspectIdentityVerificationRejected': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'ProspectIdentityVerificationSentToContingency': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'ProspectIdentityVerificationStarted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'ip_address', 'user_agent', 'is_mobile', 'idv_provider']}, 'ProspectIdentityVerificationTaken': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}}
events_keys: ['IdentityPhotosApproved', 'IdentityPhotosAgentAssigned', 'IdentityPhotosCollected', 'IdentityPhotosEvaluationStarted', 'IdentityPhotosKeptByAgent', 'IdentityPhotosStarted', 'IdentityPhotosDiscarded', 'IdentityPhotosRejected', 'IdentityPhotosDiscardedByRisk', 'IdentityWAStarted', 'IdentityWAApproved', 'IdentityWADiscarded', 'IdentityWARejected', 'IdentityWAInitialMessageResponseReceived', 'IdentityWAInputInformationCompleted', 'IdentityWADiscardedByRisk', 'IdentityWAAgentAssigned', 'IdentityWAKeptByAgent', 'ProspectIdentityVerificationDiscardedByCreditRisk', 'ProspectIdentityVerificationInitialMessageDelivered', 'ProspectIdentityVerificationInitialMessageResponseArrived', 'ProspectIdentityVerificationInitialMessageResponseReceived', 'ProspectIdentityVerificationInitialMessageStatusUpdated', 'ProspectIdentityVerificationInputInformationCompleted', 'ProspectIdentityVerificationRejected', 'ProspectIdentityVerificationSentToContingency', 'ProspectIdentityVerificationStarted', 'ProspectIdentityVerificationTaken']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
