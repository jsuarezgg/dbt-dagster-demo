{% macro return_config_co_f_refinance_condonations_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_refinance_condonations_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-11-15 15:14 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_refinance_condonations_co",
        "files_db_table_pks": [
            "loan_id"
        ]
    },
    "events": {
        "refinancetoloanapplied_by_refinanced_loan": {
            "direct_attributes": [
                "loan_id",
                "client_id",
                "condonation_id",
                "condonation_amount",
                "condonation_bucket",
                "condonation_date",
                "condonation_reason",
                "condonation_type",
                "condoned_by_origination_of_loan_id",
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
            "condoned_by_origination_of_loan_id",
            "loan_id",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}