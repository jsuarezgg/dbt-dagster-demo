{% macro return_config_d_braze_users_behaviors_subscription_group_state_logs_custom() %}
{#-target_country=;target_schema=silver;target_table_name=d_braze_users_behaviors_subscription_group_state_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-09-26 17:29 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "",
        "files_db_table_name": "d_braze_users_behaviors_subscription_group_state_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "braze_users_behaviors_subscription_group_state_change": {
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "custom_external_user_subscription_group_pairing_id",
                "external_user_id",
                "user_id",
                "app_group_id",
                "channel",
                "phone_number",
                "state_change_source",
                "subscription_group_id",
                "subscription_status",
                "timezone",
                "custom_behaviour_type"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "app_group_id",
            "channel",
            "custom_behaviour_type",
            "custom_external_user_subscription_group_pairing_id",
            "event_id",
            "external_user_id",
            "ocurred_on",
            "phone_number",
            "state_change_source",
            "subscription_group_id",
            "subscription_status",
            "timezone",
            "user_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}