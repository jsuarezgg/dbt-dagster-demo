{% macro return_config_co_f_client_survey_nps_sent_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_client_survey_nps_sent_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-07-04 16:27 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_client_survey_nps_sent_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "clientsurveynpssent": {
            "stage": "GLOBAL",
            "direct_attributes": [
                "event_name",
                "event_id",
                "ocurred_on",
                "client_id",
                "nps_date",
                "survey_type",
                "previousevents"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "client_id",
            "event_id",
            "event_name",
            "nps_date",
            "ocurred_on",
            "previousevents",
            "survey_type"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}