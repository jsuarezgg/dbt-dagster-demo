{% macro return_config_co_f_client_loan_condonations_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_client_loan_condonations_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-01-05 17:00 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_client_loan_condonations_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "clientloancondonationcreated": {
            "direct_attributes": [
                "event_id",
                "loan_id",
                "client_id",
                "condonation_id",
                "condonation_amount",
                "condonation_bucket",
                "condonation_date",
                "condonation_reason",
                "condonation_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "client_id",
            "condonation_amount",
            "condonation_bucket",
            "condonation_date",
            "condonation_id",
            "condonation_reason",
            "condonation_type",
            "event_id",
            "loan_id",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}