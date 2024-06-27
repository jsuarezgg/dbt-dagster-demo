{% macro return_config_br_f_fincore_loans_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_fincore_loans_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-28 13:18 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_fincore_loans_br_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "clientloansstatusupdatedv2_unnested_by_loan_id": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "surrogate_key",
                "event_id",
                "loan_id",
                "client_id",
                "origination_date",
                "term",
                "days_past_due",
                "paid_installments",
                "effective_annual_rate",
                "approved_amount",
                "total_principal_paid",
                "min_payment",
                "full_payment",
                "delinquency_balance",
                "principal_overdue",
                "interest_overdue",
                "guarantee_overdue",
                "unpaid_principal",
                "interest_on_overdue_principal",
                "initial_installment_amount",
                "current_installment_amount",
                "unpaid_guarantee",
                "total_payment_applied",
                "state",
                "first_payment_date",
                "months_on_book",
                "applicable_rate",
                "total_interest_paid",
                "is_fully_paid",
                "late_fees",
                "total_late_fees_paid",
                "total_late_fees_condoned",
                "custom_fincore_included",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ProspectLoanAcceptedByBankingLicensePartner": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "surrogate_key",
                "event_id",
                "loan_id",
                "client_id",
                "application_id",
                "origination_date",
                "term",
                "effective_annual_rate",
                "approved_amount",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedByBankingLicensePartnerBR": {
            "stage": "loan_acceptance_br",
            "direct_attributes": [
                "surrogate_key",
                "event_id",
                "loan_id",
                "client_id",
                "application_id",
                "origination_date",
                "term",
                "loan_type",
                "effective_annual_rate",
                "approved_amount",
                "down_payment_amount",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedWasNotSentToBankingLicensePartnerBR": {
            "stage": "loan_acceptance_br",
            "direct_attributes": [
                "surrogate_key",
                "event_id",
                "loan_id",
                "client_id",
                "application_id",
                "origination_date",
                "term",
                "loan_type",
                "effective_annual_rate",
                "approved_amount",
                "down_payment_amount",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "applicable_rate",
            "application_id",
            "approved_amount",
            "client_id",
            "current_installment_amount",
            "custom_fincore_included",
            "days_past_due",
            "delinquency_balance",
            "down_payment_amount",
            "effective_annual_rate",
            "event_id",
            "first_payment_date",
            "full_payment",
            "guarantee_overdue",
            "initial_installment_amount",
            "interest_on_overdue_principal",
            "interest_overdue",
            "is_fully_paid",
            "late_fees",
            "loan_id",
            "loan_type",
            "min_payment",
            "months_on_book",
            "ocurred_on",
            "origination_date",
            "paid_installments",
            "principal_overdue",
            "state",
            "surrogate_key",
            "term",
            "total_interest_paid",
            "total_late_fees_condoned",
            "total_late_fees_paid",
            "total_payment_applied",
            "total_principal_paid",
            "unpaid_guarantee",
            "unpaid_principal"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}
