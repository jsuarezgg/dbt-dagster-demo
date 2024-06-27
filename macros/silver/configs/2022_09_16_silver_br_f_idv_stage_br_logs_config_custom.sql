{% macro return_config_br_f_idv_stage_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_idv_stage_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-16 11:48 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_idv_stage_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "IdentityPhotosApproved": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityPhotosAgentAssigned": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityPhotosCollected": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityPhotosEvaluationStarted": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityPhotosKeptByAgent": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityPhotosStarted": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityPhotosDiscarded": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityPhotosRejected": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityPhotosDiscardedByRisk": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
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
                "observations"
            ]
        },
        "IdentityWAStarted": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id",
                "agent_user_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "is_high_risk"
            ]
        },
        "IdentityWAApproved": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations"
            ]
        },
        "IdentityWADiscarded": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations"
            ]
        },
        "IdentityWARejected": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations"
            ]
        },
        "IdentityWAInitialMessageResponseReceived": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id"
            ]
        },
        "IdentityWAInputInformationCompleted": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id"
            ]
        },
        "IdentityWADiscardedByRisk": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id",
                "reason",
                "agent_user_id",
                "used_policy_id",
                "observations"
            ]
        },
        "IdentityWAAgentAssigned": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id",
                "idv_provider"
            ]
        },
        "IdentityWAKeptByAgent": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id"
            ],
            "custom_attributes": {}
        },
        "ProspectIdentityVerificationRejected": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id"
            ]
        },
        "ProspectIdentityVerificationSentToContingency": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id"
            ]
        },
        "ProspectIdentityVerificationStarted": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ocurred_on",
                "client_id",
                "ip_address",
                "user_agent",
                "is_mobile",
                "idv_provider"
            ]
        }
    },
    "unique_db_fields": {
        "direct": [
            "agent_user_id",
            "application_id",
            "client_id",
            "event_id",
            "idv_provider",
            "ip_address",
            "is_high_risk",
            "is_mobile",
            "observations",
            "ocurred_on",
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