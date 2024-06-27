{% macro return_config_br_f_loan_cancellation_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_loan_cancellation_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-01-18 11:53 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_loan_cancellation_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "LoanCancellationOrderProcessedV2": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "loan_id",
                "client_id",
                "cancellation_date",
                "cancellation_reason",
                "cancellation_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanCancellationOrderAnnulledV2": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "loan_id",
                "client_id",
                "annulment_reason",
                "cancellation_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "annulment_reason",
            "cancellation_date",
            "cancellation_id",
            "cancellation_reason",
            "client_id",
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