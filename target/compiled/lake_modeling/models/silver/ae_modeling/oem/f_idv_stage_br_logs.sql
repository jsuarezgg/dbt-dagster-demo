
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
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosapproved_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosagentassigned_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotoscollected_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosevaluationstarted_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotoskeptbyagent_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosstarted_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosdiscarded_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosrejected_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,observations,ocurred_on,reason,used_policy_id,user_agent,
    event_name,
    event_id
    FROM identityphotosdiscardedbyrisk_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,ip_address,is_high_risk,is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,user_agent,
    event_name,
    event_id
    FROM identitywastarted_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywaapproved_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywadiscarded_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywarejected_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywainitialmessageresponsereceived_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywainputinformationcompleted_br
    UNION ALL
    SELECT 
        agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,observations,ocurred_on,reason,used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywadiscardedbyrisk_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywaagentassigned_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM identitywakeptbyagent_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationrejected_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,NULL as idv_provider,NULL as ip_address,NULL as is_high_risk,NULL as is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,NULL as user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationsenttocontingency_br
    UNION ALL
    SELECT 
        NULL as agent_user_id,application_id,client_id,idv_provider,ip_address,NULL as is_high_risk,is_mobile,NULL as observations,ocurred_on,NULL as reason,NULL as used_policy_id,user_agent,
    event_name,
    event_id
    FROM prospectidentityverificationstarted_br
    
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
this: silver.f_idv_stage_br_logs
country: br
silver_table_name: f_idv_stage_br_logs
table_pk_fields: ['event_id']
table_pk_amount: 1
fields_direct: ['agent_user_id', 'application_id', 'client_id', 'event_id', 'idv_provider', 'ip_address', 'is_high_risk', 'is_mobile', 'observations', 'ocurred_on', 'reason', 'used_policy_id', 'user_agent']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'IdentityPhotosApproved': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosAgentAssigned': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosCollected': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosEvaluationStarted': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosKeptByAgent': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosStarted': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosDiscarded': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosRejected': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityPhotosDiscardedByRisk': {'stage': 'identity_photos', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk', 'reason', 'used_policy_id', 'observations']}, 'IdentityWAStarted': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'agent_user_id', 'ip_address', 'user_agent', 'is_mobile', 'is_high_risk']}, 'IdentityWAApproved': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'IdentityWADiscarded': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'IdentityWARejected': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'IdentityWAInitialMessageResponseReceived': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'IdentityWAInputInformationCompleted': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'IdentityWADiscardedByRisk': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'reason', 'agent_user_id', 'used_policy_id', 'observations']}, 'IdentityWAAgentAssigned': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'idv_provider']}, 'IdentityWAKeptByAgent': {'stage': 'identity_wa', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id'], 'custom_attributes': {}}, 'ProspectIdentityVerificationRejected': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'ProspectIdentityVerificationSentToContingency': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id']}, 'ProspectIdentityVerificationStarted': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['event_id', 'application_id', 'ocurred_on', 'client_id', 'ip_address', 'user_agent', 'is_mobile', 'idv_provider']}}
events_keys: ['IdentityPhotosApproved', 'IdentityPhotosAgentAssigned', 'IdentityPhotosCollected', 'IdentityPhotosEvaluationStarted', 'IdentityPhotosKeptByAgent', 'IdentityPhotosStarted', 'IdentityPhotosDiscarded', 'IdentityPhotosRejected', 'IdentityPhotosDiscardedByRisk', 'IdentityWAStarted', 'IdentityWAApproved', 'IdentityWADiscarded', 'IdentityWARejected', 'IdentityWAInitialMessageResponseReceived', 'IdentityWAInputInformationCompleted', 'IdentityWADiscardedByRisk', 'IdentityWAAgentAssigned', 'IdentityWAKeptByAgent', 'ProspectIdentityVerificationRejected', 'ProspectIdentityVerificationSentToContingency', 'ProspectIdentityVerificationStarted']
flag_group_feature_active: False
version: silver_sql_builder_alternative
*/
