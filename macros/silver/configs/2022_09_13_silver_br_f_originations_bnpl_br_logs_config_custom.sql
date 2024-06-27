{% macro return_config_br_f_originations_bnpl_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_originations_bnpl_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-09 10:05 TZ-0300",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_originations_bnpl_br_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "LoanAcceptedByBankingLicensePartnerBR": {
            "stage": "loan_acceptance_br",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "loan_id",
                "origination_date",
                "approved_amount",
                "ally_slug",
                "store_slug",
                "store_user_id",
                "term",
                "loan_type",
                "effective_annual_rate",
                "lbl",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedWasNotSentToBankingLicensePartnerBR": {
            "stage": "loan_acceptance_br",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "loan_id",
                "origination_date",
                "approved_amount",
                "loan_type",
                "ally_slug",
                "store_slug",
                "store_user_id",
                "term",
                "effective_annual_rate",
                "lbl",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ProspectLoanAcceptedByBankingLicensePartner": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "loan_id",
                "origination_date",
                "approved_amount",
                "ally_slug",
                "store_slug",
                "store_user_id",
                "term",
                "effective_annual_rate",
                "lbl",
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
            "event_id",
            "lbl",
            "loan_id",
            "loan_type",
            "ocurred_on",
            "origination_date",
            "store_slug",
            "store_user_id",
            "term"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}