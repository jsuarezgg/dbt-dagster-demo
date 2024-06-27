{% macro return_config_co_f_kyc_bureau_income_validator_contributions_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_kyc_bureau_income_validator_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-12-09 11:00 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_kyc_bureau_income_validator_contributions_co_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "prospectbureauincomevalidatorobtained_unnested_by_contributions": {
            "direct_attributes": [
                "surrogate_key",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "ocurred_on",
                "income_contribution_averageIncome",
                "income_contribution_contributor_name",
                "income_contribution_contributor_type",
                "income_contribution_hasIntegralSalary",
                "income_contribution_type"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
                "surrogate_key",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "ocurred_on",
                "income_contribution_averageIncome",
                "income_contribution_contributor_name",
                "income_contribution_contributor_type",
                "income_contribution_hasIntegralSalary",
                "income_contribution_type"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}