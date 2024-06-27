{% macro return_config_br_f_origination_termination_events_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_origination_termination_events_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-07-27 12:30 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_origination_termination_events_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "applicationapproved": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationdeclined": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationexpired": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationrestarted": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "originationunexpectederroroccurred": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationclosedbynewone": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "event_id",
            "event_type",
            "journey_stage_name",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}