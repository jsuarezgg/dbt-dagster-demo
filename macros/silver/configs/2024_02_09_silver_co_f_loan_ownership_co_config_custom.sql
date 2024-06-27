{% macro return_config_co_f_loan_ownership_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_loan_ownership_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-02-09 10:23 TZ-0600",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_loan_ownership_co",
        "files_db_table_pks": [
            "loan_id"
        ]
    },
    "events": {
        "loansoldtotrust": {
            "direct_attributes": [
                "ocurred_on",
                "client_id",
                "loan_id",
                "loan_ownership",
                "sold_on",
                "sold_amount",
                "custom_is_sold",
                "custom_is_returned",
                "custom_loan_ownership_status"
            ],
            "custom_attributes": {}
        },
        "loanreturnedfromtrust": {
            "direct_attributes": [
                "ocurred_on",
                "client_id",
                "loan_id",
                "loan_ownership",
                "custom_is_sold",
                "custom_is_returned",
                "custom_loan_ownership_status"
            ],
            "custom_attributes": {}
        }  
    },
    "unique_db_fields": {
        "direct": [
                "ocurred_on",
                "client_id",
                "loan_id",
                "loan_ownership",
                "sold_on",
                "sold_amount",
                "custom_is_sold",
                "custom_is_returned",
                "custom_loan_ownership_status"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
