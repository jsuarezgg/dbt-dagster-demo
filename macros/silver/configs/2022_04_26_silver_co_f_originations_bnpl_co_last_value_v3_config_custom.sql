{% macro return_config_co_f_originations_bnpl_co_last_value_v3_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_originations_bnpl_co_last_value_v3-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-04-20 10:34 TZ-0500",
    "is_group_feature_active": True,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_originations_bnpl_co_last_value_v3",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "ProspectUpgradedToClient": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "loan_id",
                "origination_date",
                "approved_amount",
                "term",
                "interest_rate",
                "guarantee_rate",
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
                "term",
                "interest_rate",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "AllyApplicationUpdated": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "loan_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "loan_id",
                "term",
                "interest_rate",
                "guarantee_rate",
                "approved_amount",
                "origination_date",
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
                "term",
                "interest_rate",
                "guarantee_rate",
                "approved_amount",
                "ocurred_on"
            ],
            "custom_attributes": {
                "is_santander_originated": {
                    "value": "True",
                    "cast_type": "BOOLEAN"
                }
            }
        },
        "LoanAcceptedByGatewaySantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "loan_id",
                "origination_date",
                "term",
                "interest_rate",
                "guarantee_rate",
                "approved_amount",
                "ocurred_on"
            ],
            "custom_attributes": {
                "is_santander_originated": {
                    "value": "True",
                    "cast_type": "BOOLEAN"
                }
            }
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "approved_amount",
            "client_id",
            "guarantee_rate",
            "interest_rate",
            "loan_id",
            "ocurred_on",
            "origination_date",
            "term"
        ],
        "custom": [
            "is_santander_originated"
        ]
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}