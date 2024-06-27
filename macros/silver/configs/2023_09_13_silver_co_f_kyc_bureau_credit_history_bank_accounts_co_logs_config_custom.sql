{% macro return_config_co_f_kyc_bureau_credit_history_bank_accounts_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_kyc_bureau_credit_history_bank_accounts_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-09-13 15:45 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_kyc_bureau_credit_history_bank_accounts_co_logs",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "prospectbureaucredithistoryobtained_unnested_by_bank_account": {
            "direct_attributes": [
                "surrogate_key",
                "item_pseudo_idx",
                "event_id",
                "application_id",
                "client_id",
                "array_parent_path",
                "bureau_bank_account_obligationType",
                "bureau_bank_account_entityType",
                "bureau_bank_account_pastMonthlyBehaviour",
                "bureau_bank_account_originationDate",
                "bureau_bank_account_negativeInfoVisibleUntil",
                "bureau_bank_account_entityName",
                "bureau_bank_account_city",
                "bureau_bank_account_branch",
                "bureau_bank_account_obligationStatus",
                "bureau_bank_account_debtorStatus",
                "bureau_bank_account_terminationDate",
                "bureau_bank_account_obligationNumber",
                "bureau_bank_account_lastUpdateDate",
                "bureau_bank_account_rating",
                "bureau_bank_account_returnedChecks",
                "bureau_bank_account_contractType",
                "bureau_bank_account_overdraftDaysAllowed",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "array_parent_path",
            "bureau_bank_account_branch",
            "bureau_bank_account_city",
            "bureau_bank_account_contractType",
            "bureau_bank_account_debtorStatus",
            "bureau_bank_account_entityName",
            "bureau_bank_account_entityType",
            "bureau_bank_account_lastUpdateDate",
            "bureau_bank_account_negativeInfoVisibleUntil",
            "bureau_bank_account_obligationNumber",
            "bureau_bank_account_obligationStatus",
            "bureau_bank_account_obligationType",
            "bureau_bank_account_originationDate",
            "bureau_bank_account_overdraftDaysAllowed",
            "bureau_bank_account_pastMonthlyBehaviour",
            "bureau_bank_account_rating",
            "bureau_bank_account_returnedChecks",
            "bureau_bank_account_terminationDate",
            "client_id",
            "event_id",
            "item_pseudo_idx",
            "ocurred_on",
            "surrogate_key"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}