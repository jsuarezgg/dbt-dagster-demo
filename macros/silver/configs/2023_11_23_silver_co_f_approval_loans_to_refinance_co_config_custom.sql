{% macro return_config_co_f_approval_loans_to_refinance_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_approval_loans_to_refinance_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-11-23 17:53 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_approval_loans_to_refinance_co",
        "files_db_table_pks": [
            "loan_id"
        ]
    },
    "events": {
        "refinanceloanacceptedco_unnested_by_loans_to_refinance": {
            "direct_attributes": [
                "loan_id",
                "custom_loan_refinance_id",
                "application_id",
                "client_id",
                "refinanced_by_origination_of_loan_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "custom_loan_refinance_id",
            "loan_id",
            "ocurred_on",
            "refinanced_by_origination_of_loan_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}