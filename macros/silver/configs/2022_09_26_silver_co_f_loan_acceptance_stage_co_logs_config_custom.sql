{% macro return_config_co_f_loan_acceptance_stage_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_loan_acceptance_stage_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-14 17:04 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_loan_acceptance_stage_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "LoanAccepted": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ocurred_on",
                "loan_acceptance_detail_json"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ocurred_on",
                "loan_acceptance_detail_json"
            ],
            "custom_attributes": {}
        },
        "loanacceptedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ocurred_on",
                "loan_acceptance_detail_json"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "event_id",
            "loan_acceptance_detail_json",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}