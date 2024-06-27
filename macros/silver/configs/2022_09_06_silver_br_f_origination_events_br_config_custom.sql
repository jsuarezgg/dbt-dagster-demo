{% macro return_config_br_f_origination_events_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_origination_events_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-01-16 03:00 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_origination_events_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "acceptloanmaxattemptsreachedbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 0,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "additionalinformationupdatedbr": {
            "stage": "additional_information_br",
            "reference_order_id": 4,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "allyisdisabledtooriginatebr": {
            "stage": "preconditions_br",
            "reference_order_id": 5,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationapproved": {
            "stage": "GLOBAL",
            "reference_order_id": 9,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationcreated": {
            "stage": "GLOBAL",
            "reference_order_id": 10,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationdeclined": {
            "stage": "GLOBAL",
            "reference_order_id": 11,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationdeviceinformationupdated": {
            "stage": "device_information",
            "reference_order_id": 12,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationdeviceupdated": {
            "stage": "GLOBAL",
            "reference_order_id": 13,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationexpired": {
            "stage": "GLOBAL",
            "reference_order_id": 14,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationrestarted": {
            "stage": "GLOBAL",
            "reference_order_id": 15,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "backgroundcheckmaxattemptsreachedbr": {
            "stage": "background_check_br",
            "reference_order_id": 16,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "backgroundcheckpassedbr": {
            "stage": "background_check_br",
            "reference_order_id": 17,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "bankinglicensepartnercommunicationfailedbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 19,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "basicidentityvalidatedbr": {
            "stage": "basic_identity_br",
            "reference_order_id": 20,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "basicidverificationfailedbr": {
            "stage": "basic_identity_br",
            "reference_order_id": 22,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "bioinfodidnotmatchbr": {
            "stage": "basic_identity_br",
            "reference_order_id": 24,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "carddownpaymentfailedbr": {
            "stage": "down_payments_br",
            "reference_order_id": 26,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "carddownpaymentgeneratedbr": {
            "stage": "down_payments_br",
            "reference_order_id": 27,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "carddownpaymentpaidbr": {
            "stage": "down_payments_br",
            "reference_order_id": 28,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonealreadylinkedtoadifferentprospectbr": {
            "stage": "background_check_br",
            "reference_order_id": 30,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonealreadylinkedtoexistingclientbr": {
            "stage": "background_check_br",
            "reference_order_id": 32,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonelistedinblacklistbr": {
            "stage": "background_check_br",
            "reference_order_id": 34,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidatedbr": {
            "stage": "cellphone_validation_br",
            "reference_order_id": 36,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationcodedidnotmatchbr": {
            "stage": "cellphone_validation_br",
            "reference_order_id": 37,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationcodewasexpiredbr": {
            "stage": "cellphone_validation_br",
            "reference_order_id": 38,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationmaxattemptsreachedbr": {
            "stage": "cellphone_validation_br",
            "reference_order_id": 39,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationnotificationsentbr": {
            "stage": "cellphone_validation_br",
            "reference_order_id": 40,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "checkoutloginacceptancecodedidnotmatchbr": {
            "stage": "expedited_checkout_login_br",
            "reference_order_id": 41,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "checkoutloginacceptancewasexpiredbr": {
            "stage": "expedited_checkout_login_br",
            "reference_order_id": 44,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "checkoutloginacceptedbr": {
            "stage": "expedited_checkout_login_br",
            "reference_order_id": 46,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "checkoutloginsentbr": {
            "stage": "expedited_checkout_login_br",
            "reference_order_id": 48,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "checkouttransactioncompletedbr": {
            "stage": "expedited_checkout_transaction_br",
            "reference_order_id": 50,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "checkouttransactionstartedbr": {
            "stage": "expedited_checkout_transaction_br",
            "reference_order_id": 52,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientaddicupowasbalancezerobr": {
            "stage": "underwriting_rc_br",
            "reference_order_id": 54,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientautopaymenttokenizedcardstoredbr": {
            "stage": "loan_proposals_br",
            "reference_order_id": 57,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientcreditcheckfailedbr": {
            "stage": "underwriting_rc_br",
            "reference_order_id": 58,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientcreditcheckpassedbr": {
            "stage": "underwriting_rc_br",
            "reference_order_id": 61,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientfailedtoobtainscoringbr": {
            "stage": "underwriting_rc_br",
            "reference_order_id": 65,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientfraudcheckpassedbr": {
            "stage": "fraud_check_rc_br",
            "reference_order_id": 70,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientloanproposalswerewithinvalidusuryratebr": {
            "stage": "underwriting_rc_br",
            "reference_order_id": 74,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientpaymentbehaviorvalidationapprovedbr": {
            "stage": "underwriting_rc_br",
            "reference_order_id": 76,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientpaymentbehaviorvalidationrejectedbr": {
            "stage": "underwriting_rc_br",
            "reference_order_id": 79,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientpreapprovaljourneyisdisabledbr": {
            "stage": "preconditions_br",
            "reference_order_id": 82,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientwaspreapprovedbeforebr": {
            "stage": "preconditions_br",
            "reference_order_id": 84,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "conditionalcreditcheckpassedbr": {
            "stage": "underwriting_br",
            "reference_order_id": 86,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "conditionalcreditcheckpassedwithlowerapprovedamountbr": {
            "stage": "underwriting_br",
            "reference_order_id": 89,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "creditcheckfailedbr": {
            "stage": "underwriting_br",
            "reference_order_id": 93,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "creditcheckpassedbr": {
            "stage": "underwriting_br",
            "reference_order_id": 97,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "creditcheckpassedwithlowerapprovedamountbr": {
            "stage": "underwriting_br",
            "reference_order_id": 100,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "documentlistedinblacklistbr": {
            "stage": "background_check_br",
            "reference_order_id": 104,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "emailalreadylinkedtoexistingclientbr": {
            "stage": "background_check_br",
            "reference_order_id": 107,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "emaillistedinblacklistbr": {
            "stage": "background_check_br",
            "reference_order_id": 109,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "expeditedcreditcardaddedbr": {
            "stage": "expedited_add_credit_card_br",
            "reference_order_id": 112,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "expeditedcreditcardchargedbr": {
            "stage": "expedited_credit_card_charge_payment_br",
            "reference_order_id": 113,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "expeditedcreditcardchargefailedbr": {
            "stage": "expedited_credit_card_charge_payment_br",
            "reference_order_id": 114,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "expeditedcreditcardpaymentgeneratedbr": {
            "stage": "expedited_credit_card_charge_payment_br",
            "reference_order_id": 115,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "expeditedcreditcardselectedbr": {
            "stage": "expedited_select_credit_card_br",
            "reference_order_id": 116,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "expeditedcreditcardsfoundbr": {
            "stage": "expedited_get_credit_cards_br",
            "reference_order_id": 117,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "expeditednocreditcardfoundbr": {
            "stage": "expedited_get_credit_cards_br",
            "reference_order_id": 118,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "failedtoobtaincommercialinformationbr": {
            "stage": "underwriting_br",
            "reference_order_id": 120,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "failedtoobtainscoringbr": {
            "stage": "underwriting_br",
            "reference_order_id": 122,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "fraudcheckfailedbr": {
            "stage": "fraud_check_br",
            "reference_order_id": 129,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "fraudcheckpassedbr": {
            "stage": "fraud_check_br",
            "reference_order_id": 131,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosagentassigned": {
            "stage": "identity_photos",
            "reference_order_id": 135,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosapproved": {
            "stage": "identity_photos",
            "reference_order_id": 136,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotoscollected": {
            "stage": "identity_photos",
            "reference_order_id": 137,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosdiscarded": {
            "stage": "identity_photos",
            "reference_order_id": 138,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosdiscardedbyrisk": {
            "stage": "identity_photos",
            "reference_order_id": 139,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosevaluationstarted": {
            "stage": "identity_photos",
            "reference_order_id": 140,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotoskeptbyagent": {
            "stage": "identity_photos",
            "reference_order_id": 141,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosrejected": {
            "stage": "identity_photos",
            "reference_order_id": 142,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identityphotosstarted": {
            "stage": "identity_photos",
            "reference_order_id": 143,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywaagentassigned": {
            "stage": "identity_wa",
            "reference_order_id": 151,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywaapproved": {
            "stage": "identity_wa",
            "reference_order_id": 152,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywadiscarded": {
            "stage": "identity_wa",
            "reference_order_id": 153,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywadiscardedbyrisk": {
            "stage": "identity_wa",
            "reference_order_id": 154,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywainitialmessageresponsereceived": {
            "stage": "identity_wa",
            "reference_order_id": 155,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywainputinformationcompleted": {
            "stage": "identity_wa",
            "reference_order_id": 156,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywakeptbyagent": {
            "stage": "identity_wa",
            "reference_order_id": 157,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywarejected": {
            "stage": "identity_wa",
            "reference_order_id": 158,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "identitywastarted": {
            "stage": "identity_wa",
            "reference_order_id": 159,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyapproved": {
            "stage": "idv_third_party",
            "reference_order_id": 160,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartycollected": {
            "stage": "idv_third_party",
            "reference_order_id": 161,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartymanualverificationrequired": {
            "stage": "idv_third_party",
            "reference_order_id": 162,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyphotorejected": {
            "stage": "idv_third_party",
            "reference_order_id": 163,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyrejected": {
            "stage": "idv_third_party",
            "reference_order_id": 164,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyrequested": {
            "stage": "idv_third_party",
            "reference_order_id": 165,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartyskipped": {
            "stage": "idv_third_party",
            "reference_order_id": 166,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "idvthirdpartystarted": {
            "stage": "idv_third_party",
            "reference_order_id": 167,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "invalidemailbr": {
            "stage": "background_check_br",
            "reference_order_id": 168,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanacceptancecodedidnotmatchbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 173,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanacceptancenotificationfailedbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 176,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanacceptancesentbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 179,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanacceptancewasexpiredbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 182,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanacceptedbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 185,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanacceptedbybankinglicensepartnerbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 186,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanacceptedwasnotsenttobankinglicensepartnerbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 190,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanproposaldownpaymentselectedbr": {
            "stage": "loan_proposals_down_payment_br",
            "reference_order_id": 191,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanproposalselectedbr": {
            "stage": "loan_proposals_br",
            "reference_order_id": 192,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanproposalswithinvalidusuryratesbr": {
            "stage": "underwriting_br",
            "reference_order_id": 195,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanrejectedbybankinglicensepartnerbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 196,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "originationunexpectederroroccurred": {
            "stage": "GLOBAL",
            "reference_order_id": 197,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pendingdownpaymentnotifiedbr": {
            "stage": "down_payments_br",
            "reference_order_id": 198,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pendingloanproposalnotifiedbr": {
            "stage": "loan_proposals_br",
            "reference_order_id": 199,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "personalinformationupdatedbr": {
            "stage": "personal_information_br",
            "reference_order_id": 200,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixdownpaymentgeneratedbr": {
            "stage": "down_payments_br",
            "reference_order_id": 201,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixdownpaymentpaidbr": {
            "stage": "down_payments_br",
            "reference_order_id": 202,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixdownpaymentpaidbydifferentpersonbr": {
            "stage": "down_payments_br",
            "reference_order_id": 203,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixdownpaymentpaidthroughblacklistedbankbr": {
            "stage": "down_payments_br",
            "reference_order_id": 204,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixpaymentgeneratedbr": {
            "stage": "bn_pn_payments_br",
            "reference_order_id": 205,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixpaymentgenerationfailedbr": {
            "stage": "bn_pn_payments_br",
            "reference_order_id": 206,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixpaymentreceivedbr": {
            "stage": "bn_pn_payments_br",
            "reference_order_id": 207,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixpaymentrequestedbr": {
            "stage": "bn_pn_payments_br",
            "reference_order_id": 208,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preapprovalapplicationcompletedbr": {
            "stage": "preapproval_summary_br",
            "reference_order_id": 209,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preapprovalconditionalcreditcheckpassedbr": {
            "stage": "underwriting_preapproval_br",
            "reference_order_id": 212,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preapprovalcreditcheckfailedbr": {
            "stage": "underwriting_preapproval_br",
            "reference_order_id": 215,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preapprovalcreditcheckpassedbr": {
            "stage": "underwriting_preapproval_br",
            "reference_order_id": 218,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preapprovalfailedtoobtaincommercialinformationbr": {
            "stage": "underwriting_preapproval_br",
            "reference_order_id": 222,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preapprovalfailedtoobtainscoringbr": {
            "stage": "underwriting_preapproval_br",
            "reference_order_id": 224,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preconditionswerevalidbr": {
            "stage": "preconditions_br",
            "reference_order_id": 229,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "privacypolicyacceptedbr": {
            "stage": "privacy_policy_br",
            "reference_order_id": 232,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectalreadylinkedtodifferentcellphonebr": {
            "stage": "background_check_br",
            "reference_order_id": 235,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectdocumentnotfoundinbureaubr": {
            "stage": "basic_identity_br",
            "reference_order_id": 237,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectinfonotcompleteinbureaubr": {
            "stage": "basic_identity_br",
            "reference_order_id": 239,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationfailed": {
            "stage": "psychometric_assessment",
            "reference_order_id": 242,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationisrequiredbr": {
            "stage": "underwriting_br",
            "reference_order_id": 243,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationrated": {
            "stage": "psychometric_assessment",
            "reference_order_id": 246,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationstarted": {
            "stage": "psychometric_assessment",
            "reference_order_id": 248,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "requestedamountwasgreaterthanmaximumconfiguredbr": {
            "stage": "preconditions_br",
            "reference_order_id": 249,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "requestedamountwaslessthanminimumconfiguredbr": {
            "stage": "preconditions_br",
            "reference_order_id": 252,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "sendcellphonevalidationnotificationmaxattemptsreachedbr": {
            "stage": "cellphone_validation_br",
            "reference_order_id": 255,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "sendloanacceptancemaxattemptsreachedbr": {
            "stage": "loan_acceptance_br",
            "reference_order_id": 256,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "subproductselectedbr": {
            "stage": "subproduct_selection_br",
            "reference_order_id": 260,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pendingpaymentbnpnnotifiedbr": {
            "stage": "bn_pn_payments_br",
            "reference_order_id": 261,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "counterofferacceptedbr": {
            "stage": "counter_offer_br",
            "reference_order_id": 262,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "counterofferdeclinedbr": {
            "stage": "counter_offer_br",
            "reference_order_id": 263,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "counterofferfoundbr": {
            "stage": "counter_offer_br",
            "reference_order_id": 264,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "counterofferrequestedbr": {
            "stage": "underwriting_br",
            "reference_order_id": 265,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientcounterofferrequestedbr": {
            "stage": "underwriting_rc_br",
            "reference_order_id": 266,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "creditcheckpassedwithoutloan": {
            "stage": "underwriting",
            "reference_order_id": 268,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationclosedbynewone": {
            "stage": "GLOBAL",
            "reference_order_id": 269,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_stage_name",
                "event_type",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "application_id",
            "client_id",
            "event_type",
            "journey_name",
            "journey_stage_name",
            "ocurred_on"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}