{% macro return_config_co_f_loan_cancellation_requests_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_loan_cancellation_requests_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-11-23 12:04 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_loan_cancellation_requests_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "TransactionCancellationRequested": {
            "direct_attributes": [
                "ally_slug",
                "cancellation_date",
                "cancellation_reason",
                "event_id",
                "loan_id",
                "loan_source",
                "source",
                "cancellation_type",
                "cancellation_amount",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "cancellation_amount",
            "cancellation_date",
            "cancellation_reason",
            "cancellation_type",
            "event_id",
            "loan_id",
            "loan_source",
            "ocurred_on",
            "source"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}