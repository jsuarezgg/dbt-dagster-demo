{% macro return_config_br_f_underwriting_fraud_stage_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_underwriting_fraud_stage_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-02-23 14:50 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_underwriting_fraud_stage_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "ConditionalCreditCheckPassedBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "ia_overall_score",
                "bypassed_reason",
                "should_skip_idv",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "ConditionalCreditCheckPassedWithLowerApprovedAmountBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "bypassed_reason",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassedBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "ia_overall_score",
                "bypassed_reason",
                "should_skip_idv",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassedWithLowerApprovedAmountBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "bypassed_reason",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "CreditCheckFailedBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "ia_overall_score",
                "bypassed_reason",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckPassedBR": {
            "stage": "underwriting_rc_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "bypassed_reason",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckFailedBR": {
            "stage": "underwriting_rc_br",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "credit_status",
                "credit_status_reason",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "fraud_model_score",
                "learning_population",
                "bypassed_reason",
                "group_name",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationIsRequiredBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "PreApprovalConditionalCreditCheckPassedBR": {
            "stage": "underwriting_preapproval_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_product",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "credit_policy_name",
                "learning_population",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckPassedBR": {
            "stage": "underwriting_preapproval_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckFailedBR": {
            "stage": "underwriting_preapproval_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_product",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "credit_policy_name",
                "learning_population",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedByBankingLicensePartnerBR": {
            "stage": "loan_acceptance_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "FraudCheckFailedBR": {
            "stage": "fraud_check_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "fraud_model_score",
                "fraud_model_version",
                "credit_policy_name"
            ],
            "custom_attributes": {}
        },
        "FraudCheckPassedBR": {
            "stage": "fraud_check_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "fraud_model_score",
                "fraud_model_version",
                "credit_policy_name"
            ],
            "custom_attributes": {}
        },
        "ClientFraudCheckPassedBR": {
            "stage": "fraud_check_rc_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "fraud_model_score",
                "fraud_model_version",
                "credit_policy_name"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassed": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "bypassed_reason",
                "should_skip_idv",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "CreditCheckFailed": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "ConditionalCreditCheckPassed": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "credit_policy_name",
                "learning_population",
                "bypassed_reason",
                "should_skip_idv",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckPassed": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score_name",
                "credit_score_product",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "credit_score"
            ],
            "custom_attributes": {}
        },
        "ProspectUpgradedToClient": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FailedToObtainCommercialInformationBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id"
            ],
            "custom_attributes": {}
        },
        "FailedToObtainScoringBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id"
            ],
            "custom_attributes": {}
        },
        "LoanProposalSelectedBR": {
            "stage": "loan_proposals_br",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id"
            ],
            "custom_attributes": {}
        },
        "LoanProposalsWithInvalidUsuryRatesBR": {
            "stage": "underwriting_br",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "bypassed_reason",
            "client_id",
            "client_max_exposure",
            "credit_check_income_net_value",
            "credit_check_income_provider",
            "credit_check_income_type",
            "credit_policy_name",
            "credit_score",
            "credit_score_name",
            "credit_score_product",
            "credit_status",
            "credit_status_reason",
            "fraud_model_score",
            "fraud_model_version",
            "group_name",
            "ia_overall_score",
            "learning_population",
            "ocurred_on",
            "pd_calculation_method",
            "probability_default_addi",
            "probability_default_bureau",
            "should_skip_idv",
            "store_fraud_risk_level",
            "tdsr"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}