{% macro return_config_co_f_originations_bnpl_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_originations_bnpl_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-05-07 09:49 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_originations_bnpl_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "LoanAcceptedCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
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
                "guarantee_rate",
                "guarantee_provider",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FailedToSendSignedFilesSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
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
                "guarantee_rate",
                "ocurred_on",
                "custom_is_santander_originated"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedByGatewaySantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
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
                "guarantee_rate",
                "ocurred_on",
                "custom_is_santander_originated"
            ],
            "custom_attributes": {}
        },
        "AllyApplicationUpdated": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "loan_id",
                "ally_slug",
                "store_user_id",
                "store_slug",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ProspectUpgradedToClient": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
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
                "guarantee_rate",
                "lbl",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientLoanAccepted": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
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
            "custom_is_santander_originated",
            "effective_annual_rate",
            "guarantee_provider",
            "guarantee_rate",
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