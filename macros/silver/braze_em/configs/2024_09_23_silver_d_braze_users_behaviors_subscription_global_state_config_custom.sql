{% macro return_config_d_braze_users_behaviors_subscription_global_state_custom() %}
{#-target_country=;target_schema=silver;target_table_name=d_braze_users_behaviors_subscription_global_state-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-09-26 17:29 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "",
        "files_db_table_name": "d_braze_users_behaviors_subscription_global_state",
        "files_db_table_pks": [
            "custom_external_user_channel_pairing_id"
        ]
    },
    "events": {
        "braze_users_behaviors_subscription_global_state_change": {
            "direct_attributes": [
                "ocurred_on",
                "custom_external_user_channel_pairing_id",
                "external_user_id",
                "user_id",
                "app_group_id",
                "campaign_id",
                "campaign_name",
                "canvas_id",
                "canvas_name",
                "canvas_step_id",
                "canvas_step_name",
                "canvas_variation_id",
                "canvas_variation_name",
                "channel",
                "email_address",
                "message_variation_id",
                "state_change_source",
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
            "campaign_id",
            "campaign_name",
            "canvas_id",
            "canvas_name",
            "canvas_step_id",
            "canvas_step_name",
            "canvas_variation_id",
            "canvas_variation_name",
            "channel",
            "custom_behaviour_type",
            "custom_external_user_channel_pairing_id",
            "email_address",
            "external_user_id",
            "message_variation_id",
            "ocurred_on",
            "state_change_source",
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