{% macro return_config_co_funnel_stages_imply() %}
{#-target_country=co;target_schema=imply;target_table_name=funnel_stages_imply-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-05-19 16:20 TZ-0500",
    "is_group_feature_active": False,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "silver_co_funnel_stages",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "ApplicationCreated": {
            "stage": "GLOBAL",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ApplicationDeclined": {
            "stage": "GLOBAL",
            "direct_attributes": [
                "event_id",
                "application_id",
                "ally_name",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ApplicationExpired": {
            "stage": "GLOBAL",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientWasPreapprovedBeforePagoCO": {
            "stage": "preconditions_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientPreapprovalJourneyIsDisabledPagoCO": {
            "stage": "preconditions_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "RequestedAmountWasGreaterThanMaximumConfiguredPagoCO": {
            "stage": "preconditions_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "RequestedAmountWasLessThanMinimumConfiguredPagoCO": {
            "stage": "preconditions_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "OriginationUnexpectedErrorOccurred": {
            "stage": "GLOBAL",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "AllyIsDisabledToOriginateCO": {
            "stage": "preconditions_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "AllyIsDisabledToOriginatePagoCO": {
            "stage": "preconditions_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LeadDisabledToOriginatePagoCO": {
            "stage": "preconditions_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FailedToValidateEmailCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreconditionsWereValidCO": {
            "stage": "preconditions_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreconditionsWereValidPagoCO": {
            "stage": "preconditions_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "RequestedAmountWasGreaterThanMaximumConfiguredCO": {
            "stage": "preconditions_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "RequestedAmountWasLessThanMinimumConfiguredCO": {
            "stage": "preconditions_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LeadDisabledToOriginateCO": {
            "stage": "preconditions_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PrivacyPolicyAcceptedCO": {
            "stage": "privacy_policy_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "BackgroundCheckPassedCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CellphoneAlreadyLinkedToADifferentProspectCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CellphoneAlreadyLinkedToExistingClientCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CellphoneListedInBlackListCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "DocumentListedInBlackListCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "InvalidEmailCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ListedInOFACCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ProspectAlreadyLinkedToDifferentCellphoneCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "BasicIdVerificationFailedCO": {
            "stage": "basic_identity_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "BasicIdentityValidatedCO": {
            "stage": "basic_identity_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "BioInfoDidNotMatchCO": {
            "stage": "basic_identity_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ProspectDocumentNotFoundInBureauCO": {
            "stage": "basic_identity_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ProspectInfoNotCompleteInBureauCO": {
            "stage": "basic_identity_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientHasFootprintFromBlacklistedEntityCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ConditionalCreditCheckPassedCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckFailedCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassedCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FailedToObtainCommercialInformationCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FailedToObtainScoringCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientFailedToObtainBureauInformationPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientFailedToObtainScoringPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientPaymentBehaviorValidationApprovedPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientPaymentBehaviorValidationRejectedPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientAddiCupoWasBalanceZeroCO": {
            "stage": "underwriting_rc_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckFailedCO": {
            "stage": "underwriting_rc_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckPassedCO": {
            "stage": "underwriting_rc_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientFailedToObtainScoringCO": {
            "stage": "underwriting_rc_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientPaymentBehaviorValidationApprovedCO": {
            "stage": "underwriting_rc_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientPaymentBehaviorValidationRejectedCO": {
            "stage": "underwriting_rc_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalConditionalCreditCheckPassedPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckFailedPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckPassedPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalFailedToObtainBureauInformationPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalFailedToObtainScoringPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalLoanProposalsWithInvalidUsuryRatesPagoCO": {
            "stage": "underwriting_preapproval_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationApproved": {
            "stage": "psychometric_assessment",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationRated": {
            "stage": "psychometric_assessment",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationFailed": {
            "stage": "psychometric_assessment",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationRejected": {
            "stage": "psychometric_assessment",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationStarted": {
            "stage": "psychometric_assessment",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationIsRequiredCO": {
            "stage": "underwriting_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientAddiCupoWasBalanceZeroPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckFailedPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientCreditCheckPassedPagoCO": {
            "stage": "underwriting_rc_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanProposalSelectedCO": {
            "stage": "loan_proposals_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FirstPaymentDateChangedCO": {
            "stage": "loan_proposals_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWAAgentAssigned": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWAApproved": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWADiscarded": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWADiscardedByRisk": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWAInitialMessageResponseReceived": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWAInputInformationCompleted": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWARejected": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWAStarted": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "AcceptLoanMaxAttemptsReachedCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptanceCodeDidNotMatchCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptanceNotificationFailedCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptanceSentCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptanceWasExpiredCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "SendLoanAcceptanceMaxAttemptsReachedCO": {
            "stage": "loan_acceptance_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreapprovalApplicationCompletedCO": {
            "stage": "preapproval_summary_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ProspectApplicationPreapproved": {
            "stage": "legacy_or_non_origination",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PrivacyPolicyAcceptedSantanderCO": {
            "stage": "privacy_policy_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckApprovedSantanderCO": {
            "stage": "risk_evaluation_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckFailedSantanderCO": {
            "stage": "risk_evaluation_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckRejectedSantanderCO": {
            "stage": "risk_evaluation_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanProposalSelectedSantanderCO": {
            "stage": "loan_proposals_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "WorkInformationReceivedSantanderCO": {
            "stage": "work_information_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "AcceptLoanMaxAttemptsReachedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CaseCreatedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientInformationObtainedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreateCaseFailedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "DocumentsObtainedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FailedToSignDocumentsSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FailedToSendSignedFilesSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "GetClientInformationFailedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "GetDocumentsFailedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptanceCodeDidNotMatchSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptanceNotificationFailedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptanceSentSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptanceWasExpiredSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedByGatewaySantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "LoanAcceptedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "SendLoanAcceptanceMaxAttemptsReachedSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "SignedDocumentsSantanderCO": {
            "stage": "loan_acceptance_santander_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalClientHasFootprintFromBlacklistedEntityCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalConditionalCreditCheckPassedCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckFailedCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalCreditCheckPassedCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalFailedToObtainCommercialInformationCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalFailedToObtainScoringCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PreApprovalLoanProposalsWithInvalidUsuryRatesCO": {
            "stage": "underwriting_preapproval_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ConditionalCreditCheckPsychometricPassedCO": {
            "stage": "underwriting_psychometric_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPsychometricPassedCO": {
            "stage": "underwriting_psychometric_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPsychometricFailedCO": {
            "stage": "underwriting_psychometric_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ConditionalCreditCheckPassedPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckFailedPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CreditCheckPassedPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FailedToObtainScoringPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "PsychometricEvaluationIsRequiredPagoCO": {
            "stage": "underwriting_pago_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ApplicationRestarted": {
            "stage": "GLOBAL",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityWAKeptByAgent": {
            "stage": "identity_wa",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "EmailAlreadyLinkedToExistingClientCO": {
            "stage": "background_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FraudCheckFailedCO": {
            "stage": "fraud_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientFraudCheckFailedCO": {
            "stage": "fraud_check_rc_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "FraudCheckPassedCO": {
            "stage": "fraud_check_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ClientFraudCheckPassedCO": {
            "stage": "fraud_check_rc_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ApplicationDeviceInformationUpdated": {
            "stage": "device_information",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosApproved": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosAgentAssigned": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosCollected": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosEvaluationStarted": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosKeptByAgent": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosStarted": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosDiscarded": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosRejected": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "IdentityPhotosDiscardedByRisk": {
            "stage": "identity_photos",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CheckoutLoginAcceptanceCodeDidNotMatchCO": {
            "stage": "expedited_checkout_login_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CheckoutLoginAcceptanceNotificationFailedCO": {
            "stage": "expedited_checkout_login_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CheckoutLoginAcceptanceWasExpiredCO": {
            "stage": "expedited_checkout_login_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CheckoutLoginAcceptedCO": {
            "stage": "expedited_checkout_login_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CheckoutLoginSentCO": {
            "stage": "expedited_checkout_login_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CheckoutTransactionCompletedCO": {
            "stage": "expedited_checkout_transaction_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "CheckoutTransactionStartedCO": {
            "stage": "expedited_checkout_transaction_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ExpeditedCheckoutLoanProposalSelectedCO": {
            "stage": "expedited_loan_proposals_co",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "ApplicationAdjustedCO": {
            "stage": "GLOBAL",
            "direct_attributes": [
                "event_id",
                "application_id",
                "client_id",
                "ally_name",
                "client_type",
                "event_type",
                "journey_stage_name",
                "journey_name",
                "product",
                "channel",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
    },
    "unique_db_fields": {
        "direct": [
            "ally_name",
            "application_id",
            "channel",
            "client_id",
            "client_type",
            "event_id",
            "event_type",
            "journey_name",
            "journey_stage_name",
            "ocurred_on",
            "product"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}