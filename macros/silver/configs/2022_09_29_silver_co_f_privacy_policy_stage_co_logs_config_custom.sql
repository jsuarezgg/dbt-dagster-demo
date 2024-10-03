{% macro return_config_co_f_privacy_policy_stage_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_privacy_policy_stage_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-07-23 12:04 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_privacy_policy_stage_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "PrivacyPolicyAccepted": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "application_id",
                "client_id",
                "custom_is_privacy_policy_accepted",
                "otp_code"
            ],
            "custom_attributes": {}
        },
        "PrivacyPolicyAcceptedCO": {
            "stage": "privacy_policy_co",
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "application_id",
                "client_id",
                "custom_is_privacy_policy_accepted",
                "otp_code"
            ],
            "custom_attributes": {}
        },
        "privacypolicyacceptedsantanderco": {
            "stage": "privacy_policy_santander_co",
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "application_id",
                "client_id",
                "custom_is_privacy_policy_accepted",
                "otp_code"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "event_id",
            "ocurred_on",
            "custom_is_privacy_policy_accepted",
            "otp_code"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
