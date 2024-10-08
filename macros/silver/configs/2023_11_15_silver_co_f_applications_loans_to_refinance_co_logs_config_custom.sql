{% macro return_config_co_f_applications_loans_to_refinance_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_loans_to_refinance_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-11-23 17:53 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_loans_to_refinance_co_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "applicationcreated_unnested_by_loans_to_refinance": {
            "direct_attributes": [
                "surrogate_key",
                "custom_loan_refinance_id",
                "loan_id",
                "application_id",
                "client_id",
                "is_eligible_for_refinance",
                "outstanding_balance_total",
                "outstanding_balance_unpaid_principal",
                "outstanding_balance_interest_overdue",
                "outstanding_balance_collection_fees",
                "outstanding_balance_moratory_interest",
                "paid_guarantee_at_refinance_application",
                "paid_interest_at_refinance_application",
                "paid_collection_fees_at_refinance_application",
                "paid_principal_at_refinance_application",
                "total_payment_applied_at_refinance_application",
                "total_prepayment_benefit_at_refinance_application",
                "unpaid_guarantee_at_refinance_application",
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
            "is_eligible_for_refinance",
            "loan_id",
            "ocurred_on",
            "outstanding_balance_collection_fees",
            "outstanding_balance_interest_overdue",
            "outstanding_balance_moratory_interest",
            "outstanding_balance_total",
            "outstanding_balance_unpaid_principal",
            "paid_collection_fees_at_refinance_application",
            "paid_guarantee_at_refinance_application",
            "paid_interest_at_refinance_application",
            "paid_principal_at_refinance_application",
            "surrogate_key",
            "total_payment_applied_at_refinance_application",
            "total_prepayment_benefit_at_refinance_application",
            "unpaid_guarantee_at_refinance_application"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}