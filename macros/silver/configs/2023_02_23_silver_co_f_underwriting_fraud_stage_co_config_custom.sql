{% macro return_config_co_f_underwriting_fraud_stage_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_underwriting_fraud_stage_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-03-12 16:51 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_underwriting_fraud_stage_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "ConditionalCreditCheckPassedCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
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
                "lbl"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassedCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl",
                "bypassed_reason"
            ],
            "custom_attributes": {}
        },
        "CreditCheckFailedCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "ia_overall_score",
                "lbl",
                "bypassed_reason"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckPassedCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "lbl"
            ],
            "custom_attributes": {}
        },
        "PreApprovalConditionalCreditCheckPassedCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "lbl",
                "bypassed_reason"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckFailedCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "lbl"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckFailedPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "evaluation_type",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckPassedCO": {
            "stage": "underwriting_rc_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl",
                "bypassed_reason"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckFailedCO": {
            "stage": "underwriting_rc_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl",
                "bypassed_reason"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckPassedPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "evaluation_type",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "ConditionalClientCreditCheckPassedPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "evaluation_type",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "CreditCheckApprovedSantanderCO": {
            "stage": "risk_evaluation_santander_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "group_name",
                "client_max_exposure",
                "tdsr",
                "learning_population",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationIsRequiredCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "ia_overall_score",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "PreApprovalConditionalCreditCheckPassedPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckFailedPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckPassedPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationApproved": {
            "stage": "psychometric_assessment",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "ia_overall_score",
                "bypassed_reason",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "ConditionalCreditCheckPsychometricPassedCO": {
            "stage": "underwriting_psychometric_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPsychometricPassedCO": {
            "stage": "underwriting_psychometric_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "ia_overall_score",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPsychometricFailedCO": {
            "stage": "underwriting_psychometric_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "ia_overall_score",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "ConditionalCreditCheckPassedPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score_name",
                "evaluation_type",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "ia_overall_score",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "CreditCheckFailedPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "evaluation_type",
                "should_be_black_listed",
                "credit_score_name",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl",
                "policy",
                "store_fraud_risk_level",
                "fraud_model_score",
                "fraud_model_version"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassedPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "should_be_black_listed",
                "credit_score_name",
                "evaluation_type",
                "probability_default_bureau",
                "probability_default_addi",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "policy",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "ia_overall_score",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationIsRequiredPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
                "credit_score_name",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "lbl"
            ],
            "custom_attributes": {}
        },
        "FraudCheckFailedCO": {
            "stage": "fraud_check_co",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "fraud_model_score",
                "fraud_model_version"
            ],
            "custom_attributes": {}
        },
        "FraudCheckPassedCO": {
            "stage": "fraud_check_co",
            "direct_attributes": [
                "application_id",
                "ocurred_on",
                "client_id",
                "fraud_model_score",
                "fraud_model_version"
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
                "credit_score",
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
                "credit_check_income_validator_calculation_method",
                "credit_check_income_validator_contribution_type",
                "tdsr",
                "credit_policy_name",
                "learning_population"
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
                "credit_score",
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
                "credit_check_income_validator_calculation_method",
                "credit_check_income_validator_contribution_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "bypassed_reason",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassedWithLowerApprovedAmount": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "credit_check_income_validator_calculation_method",
                "credit_check_income_validator_contribution_type",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population"
            ],
            "custom_attributes": {}
        },
        "ConditionalCreditCheckPassedWithLowerApprovedAmount": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "credit_check_income_validator_calculation_method",
                "credit_check_income_validator_contribution_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "learning_population",
                "bypassed_reason",
                "should_skip_idv",
                "lbl"
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
                "credit_score",
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
                "credit_check_income_validator_calculation_method",
                "credit_check_income_validator_contribution_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckPassedWithLowerApprovedAmount": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "credit_check_income_validator_calculation_method",
                "credit_check_income_validator_contribution_type",
                "tdsr",
                "pd_calculation_method",
                "credit_policy_name",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckPassedWithInsufficientAddiCupo": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
                "credit_score_name",
                "group_name",
                "fraud_model_score",
                "fraud_model_version",
                "store_fraud_risk_level",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "tdsr",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckFailed": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "credit_status",
                "credit_status_reason",
                "credit_score",
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
                "credit_policy_name"
            ],
            "custom_attributes": {}
        },
        "ProspectUpgradedToClient": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "client_id",
                "ocurred_on",
                "client_max_exposure",
                "lbl"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassed": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "application_id",
                "bypassed_reason",
                "client_max_exposure",
                "credit_check_income_net_value",
                "credit_check_income_provider",
                "credit_check_income_type",
                "credit_check_income_validator_calculation_method",
                "credit_check_income_validator_contribution_type",
                "credit_policy_name",
                "credit_score",
                "credit_score_name",
                "credit_score_product",
                "credit_status",
                "credit_status_reason",
                "fraud_model_score",
                "fraud_model_version",
                "group_name",
                "learning_population",
                "lbl",
                "ocurred_on",
                "pd_calculation_method",
                "probability_default_addi",
                "probability_default_bureau",
                "client_id",
                "should_skip_idv",
                "store_fraud_risk_level",
                "tdsr"
            ],
            "custom_attributes": {}
        },
        "failedtocreateloanrefinanceproposalsco": {
            "stage": "refinance_loan_proposals_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "credit_status",
                "credit_status_reason",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanrefinanceproposalscreatedco": {
            "stage": "refinance_loan_proposals_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "credit_policy_name",
                "credit_status",
                "credit_status_reason",
                "client_max_exposure",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "refinanceloanacceptedco": {
            "stage": "refinance_loan_acceptance_co",
            "direct_attributes": [
                "application_id",
                "client_id",
                "credit_status",
                "credit_status_reason",
                "client_max_exposure",
                "credit_policy_name",
                "lbl",
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
            "credit_check_income_validator_calculation_method",
            "credit_check_income_validator_contribution_type",
            "credit_policy_name",
            "credit_score",
            "credit_score_name",
            "credit_score_product",
            "credit_status",
            "credit_status_reason",
            "evaluation_type",
            "fraud_model_score",
            "fraud_model_version",
            "group_name",
            "ia_overall_score",
            "lbl",
            "learning_population",
            "ocurred_on",
            "pd_calculation_method",
            "policy",
            "probability_default_addi",
            "probability_default_bureau",
            "should_be_black_listed",
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