{% macro return_config_br_f_kyc_bureau_income_estimator_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_kyc_bureau_income_estimator_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-12-09 11:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_kyc_bureau_income_estimator_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "prospectbureauincomeestimatorobtained": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "income_estimatedIncome",
                "income_paymentCapacity",
                "metadata_context_traceId",
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
            "income_estimatedIncome",
            "income_paymentCapacity",
            "metadata_context_traceId",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}