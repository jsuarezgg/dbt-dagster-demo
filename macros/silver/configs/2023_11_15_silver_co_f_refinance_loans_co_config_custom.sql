{% macro return_config_co_f_refinance_loans_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_refinance_loans_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-11-15 15:14 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_refinance_loans_co",
        "files_db_table_pks": [
            "loan_id"
        ]
    },
    "events": {
        "refinanceloanacceptedco": {
            "direct_attributes": [
                "loan_id",
                "application_id",
                "client_id",
                "ally_slug",
                "store_slug",
                "store_user_id",
                "approved_amount",
                "effective_annual_rate",
                "first_payment_date",
                "guarantee_total",
                "guarantee_rate",
                "origination_date",
                "term",
                "total_interest",
                "loan_type",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "approved_amount",
            "client_id",
            "effective_annual_rate",
            "first_payment_date",
            "guarantee_rate",
            "guarantee_total",
            "loan_id",
            "loan_type",
            "ocurred_on",
            "origination_date",
            "store_slug",
            "store_user_id",
            "term",
            "total_interest"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}