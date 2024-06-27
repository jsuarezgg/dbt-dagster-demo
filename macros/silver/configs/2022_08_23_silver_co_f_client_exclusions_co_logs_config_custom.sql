{% macro return_config_co_f_client_exclusions_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_client_exclusions_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-08-23 15:29 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_client_exclusions_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "ClientAddedToExclusionList": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "client_id",
                "loan_id",
                "reason",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "client_id",
            "event_id",
            "loan_id",
            "ocurred_on",
            "reason"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
