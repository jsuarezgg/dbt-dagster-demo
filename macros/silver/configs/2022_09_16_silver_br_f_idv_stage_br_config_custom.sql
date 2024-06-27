{% macro return_config_br_f_idv_stage_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_idv_stage_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-16 11:48 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_idv_stage_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "IdentityPhotosApproved": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotosapproved_at"
            ]
        },
        "IdentityPhotosAgentAssigned": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotosagentassigned_at"
            ]
        },
        "IdentityPhotosCollected": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotoscollected_at"
            ]
        },
        "IdentityPhotosEvaluationStarted": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotosevaluationstarted_at"
            ]
        },
        "IdentityPhotosKeptByAgent": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotoskeptbyagent_at"
            ]
        },
        "IdentityPhotosStarted": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotosstarted_at"
            ]
        },
        "IdentityPhotosDiscarded": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotosdiscarded_at"
            ]
        },
        "IdentityPhotosRejected": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotosrejected_at"
            ]
        },
        "IdentityPhotosDiscardedByRisk": {
            "stage": "identity_photos",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "reason",
                "used_policy_id",
                "observations",
                "identityphotosdiscardedbyrisk_at"
            ]
        },
        "IdentityWAStarted": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk",
                "identitywastarted_at"
            ]
        },
        "IdentityWAApproved": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations",
                "identitywaapproved_at"
            ]
        },
        "IdentityWADiscarded": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations",
                "identitywadiscarded_at"
            ]
        },
        "IdentityWARejected": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations",
                "identitywarejected_at"
            ]
        },
        "IdentityWAInitialMessageResponseReceived": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "identitywainitialmessageresponsereceived_at"
            ]
        },
        "IdentityWAInputInformationCompleted": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "identitywainputinformationcompleted_at"
            ]
        },
        "IdentityWADiscardedByRisk": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations",
                "identitywadiscardedbyrisk_at"
            ]
        },
        "IdentityWAAgentAssigned": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "idv_provider",
                "identitywaagentassigned_at"
            ]
        },
        "IdentityWAKeptByAgent": {
            "stage": "identity_wa",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "identitywakeptbyagent_at"
            ],
            "custom_attributes": {}
        },
        "ProspectIdentityVerificationRejected": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "prospectidentityverificationrejected_at"
            ]
        },
        "ProspectIdentityVerificationSentToContingency": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "prospectidentityverificationsenttocontingency_at"
            ]
        },
        "ProspectIdentityVerificationStarted": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "idv_provider",
                "prospectidentityverificationstarted_at"
            ]
        }
    },
    "unique_db_fields": {
        "direct": [
            "agent_user_id",
            "application_id",
            "client_id",
            "identityphotosagentassigned_at",
            "identityphotosapproved_at",
            "identityphotoscollected_at",
            "identityphotosdiscarded_at",
            "identityphotosdiscardedbyrisk_at",
            "identityphotosevaluationstarted_at",
            "identityphotoskeptbyagent_at",
            "identityphotosrejected_at",
            "identityphotosstarted_at",
            "identitywaagentassigned_at",
            "identitywaapproved_at",
            "identitywadiscarded_at",
            "identitywadiscardedbyrisk_at",
            "identitywainitialmessageresponsereceived_at",
            "identitywainputinformationcompleted_at",
            "identitywakeptbyagent_at",
            "identitywarejected_at",
            "identitywastarted_at",
            "idv_provider",
            "ip_address",
            "is_high_risk",
            "is_mobile",
            "observations",
            "ocurred_on",
            "prospectidentityverificationrejected_at",
            "prospectidentityverificationsenttocontingency_at",
            "prospectidentityverificationstarted_at",
            "reason",
            "used_policy_id",
            "user_agent"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}