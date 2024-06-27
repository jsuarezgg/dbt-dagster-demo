{% macro return_config_co_f_device_information_stage_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_device_information_stage_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-16 17:04 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_device_information_stage_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "ApplicationDeviceUpdated": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "ocurred_on",
                "user_agent"
            ],
            "custom_attributes": {}
        },
        "ApplicationDeviceInformationUpdated": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "ocurred_on",
                "user_agent",
                "device_id"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "event_id",
            "ocurred_on",
            "user_agent",
            "device_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}