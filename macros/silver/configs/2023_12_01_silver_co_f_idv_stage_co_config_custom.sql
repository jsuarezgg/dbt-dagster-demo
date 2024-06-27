{% macro return_config_co_f_idv_stage_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_idv_stage_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-12-01 16:08 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_idv_stage_co",
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
                "identityphotosapproved_at",
                "photo_quality"
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
                "identityphotosdiscarded_at",
                "photo_quality"
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
                "identityphotosrejected_at",
                "photo_quality"
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
                "identityphotosdiscardedbyrisk_at",
                "photo_quality"
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
                "agent_user_id",
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
        "ProspectIdentityVerificationDiscardedByCreditRisk": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations",
                "prospectidentityverificationdiscardedbycreditrisk_at"
            ]
        },
        "ProspectIdentityVerificationInitialMessageDelivered": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "prospectidentityverificationinitialmessagedelivered_at"
            ]
        },
        "ProspectIdentityVerificationInitialMessageResponseArrived": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "prospectidentityverificationinitialmessageresponsearrived_at"
            ]
        },
        "ProspectIdentityVerificationInitialMessageResponseReceived": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "prospectidentityverificationinitialmessageresponsereceived_at"
            ]
        },
        "ProspectIdentityVerificationInitialMessageStatusUpdated": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "prospectidentityverificationinitialmessagestatusupdated_at"
            ]
        },
        "ProspectIdentityVerificationInputInformationCompleted": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "prospectidentityverificationinputinformationcompleted_at"
            ]
        },
        "ProspectIdentityVerificationRejected": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations",
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
        },
        "ProspectIdentityVerificationTaken": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "prospectidentityverificationtaken_at"
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
            "photo_quality",
            "prospectidentityverificationdiscardedbycreditrisk_at",
            "prospectidentityverificationinitialmessagedelivered_at",
            "prospectidentityverificationinitialmessageresponsearrived_at",
            "prospectidentityverificationinitialmessageresponsereceived_at",
            "prospectidentityverificationinitialmessagestatusupdated_at",
            "prospectidentityverificationinputinformationcompleted_at",
            "prospectidentityverificationrejected_at",
            "prospectidentityverificationsenttocontingency_at",
            "prospectidentityverificationstarted_at",
            "prospectidentityverificationtaken_at",
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