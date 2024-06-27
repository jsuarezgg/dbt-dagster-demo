{% macro return_config_co_f_ally_management_activation_events_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_ally_management_activation_events_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-07-05 11:30 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_ally_management_activation_events_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "allyactivationprocessfinishedwitherror": {
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "ally_slug"
            ],
            "custom_attributes": {}
        },
        "allyactivationprocessfinishedsuccessful": {
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "ally_slug"
            ],
            "custom_attributes": {}
        },
        "allytermsandconditionsprocessaccepted": {
            "direct_attributes": [
                "event_id",
                "ocurred_on",
                "ally_slug"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "event_id",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}