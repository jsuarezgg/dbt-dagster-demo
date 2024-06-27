{% macro return_config_co_f_origination_events_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_origination_events_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-12-21 16:29 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_origination_events_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "acceptloanmaxattemptsreachedco": {
            "stage": "loan_acceptance_co",
            "reference_order_id": 1,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "acceptloanmaxattemptsreachedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 2,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "additionalinformationreceivedsantanderco": {
            "stage": "additional_information_santander_co",
            "reference_order_id": 3,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "allyisdisabledtooriginateco": {
            "stage": "preconditions_co",
            "reference_order_id": 6,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "allyisdisabledtooriginatepagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 7,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationadjustedco": {
            "stage": "GLOBAL",
            "reference_order_id": 8,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationcreated": {
            "stage": "GLOBAL",
            "reference_order_id": 10,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationdeclined": {
            "stage": "GLOBAL",
            "reference_order_id": 11,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationdeviceinformationupdated": {
            "stage": "device_information",
            "reference_order_id": 12,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationdeviceupdated": {
            "stage": "GLOBAL",
            "reference_order_id": 13,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationexpired": {
            "stage": "GLOBAL",
            "reference_order_id": 14,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationrestarted": {
            "stage": "GLOBAL",
            "reference_order_id": 15,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "backgroundcheckpassedco": {
            "stage": "background_check_co",
            "reference_order_id": 18,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "basicidentityvalidatedco": {
            "stage": "basic_identity_co",
            "reference_order_id": 21,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "basicidverificationfailedco": {
            "stage": "basic_identity_co",
            "reference_order_id": 23,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "bioinfodidnotmatchco": {
            "stage": "basic_identity_co",
            "reference_order_id": 25,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "casecreatedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 29,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "cellphonealreadylinkedtoadifferentprospectco": {
            "stage": "background_check_co",
            "reference_order_id": 31,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "cellphonealreadylinkedtoexistingclientco": {
            "stage": "background_check_co",
            "reference_order_id": 33,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "cellphonelistedinblacklistco": {
            "stage": "background_check_co",
            "reference_order_id": 35,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "checkoutloginacceptancecodedidnotmatchco": {
            "stage": "expedited_checkout_login_co",
            "reference_order_id": 42,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "checkoutloginacceptancenotificationfailedco": {
            "stage": "expedited_checkout_login_co",
            "reference_order_id": 43,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "checkoutloginacceptancewasexpiredco": {
            "stage": "expedited_checkout_login_co",
            "reference_order_id": 45,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "checkoutloginacceptedco": {
            "stage": "expedited_checkout_login_co",
            "reference_order_id": 47,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "checkoutloginsentco": {
            "stage": "expedited_checkout_login_co",
            "reference_order_id": 49,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "checkouttransactioncompletedco": {
            "stage": "expedited_checkout_transaction_co",
            "reference_order_id": 51,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "checkouttransactionstartedco": {
            "stage": "expedited_checkout_transaction_co",
            "reference_order_id": 53,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientaddicupowasbalancezeroco": {
            "stage": "underwriting_rc_co",
            "reference_order_id": 55,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientaddicupowasbalancezeropagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 56,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientcreditcheckfailedco": {
            "stage": "underwriting_rc_co",
            "reference_order_id": 59,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientcreditcheckfailedpagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 60,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientcreditcheckpassedco": {
            "stage": "underwriting_rc_co",
            "reference_order_id": 62,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientcreditcheckpassedpagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 63,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientfailedtoobtainbureauinformationpagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 64,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientfailedtoobtainscoringco": {
            "stage": "underwriting_rc_co",
            "reference_order_id": 66,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientfailedtoobtainscoringpagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 67,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientfirstlastnameconfirmedco": {
            "stage": "privacy_policy_co",
            "reference_order_id": 68,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientfraudcheckfailedco": {
            "stage": "fraud_check_rc_co",
            "reference_order_id": 69,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientfraudcheckpassedco": {
            "stage": "fraud_check_rc_co",
            "reference_order_id": 71,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clienthasfootprintfromblacklistedentityco": {
            "stage": "underwriting_co",
            "reference_order_id": 72,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientinformationobtainedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 73,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientnationalidentificationexpeditiondateconfirmedco": {
            "stage": "privacy_policy_co",
            "reference_order_id": 75,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientpaymentbehaviorvalidationapprovedco": {
            "stage": "underwriting_rc_co",
            "reference_order_id": 77,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientpaymentbehaviorvalidationapprovedpagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 78,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientpaymentbehaviorvalidationrejectedco": {
            "stage": "underwriting_rc_co",
            "reference_order_id": 80,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientpaymentbehaviorvalidationrejectedpagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 81,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientpreapprovaljourneyisdisabledpagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 83,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientwaspreapprovedbeforepagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 85,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "conditionalcreditcheckpassedco": {
            "stage": "underwriting_co",
            "reference_order_id": 87,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "conditionalcreditcheckpassedpagoco": {
            "stage": "underwriting_pago_co",
            "reference_order_id": 88,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "conditionalcreditcheckpsychometricpassedco": {
            "stage": "underwriting_psychometric_co",
            "reference_order_id": 90,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "createcasefailedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 91,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckapprovedsantanderco": {
            "stage": "risk_evaluation_santander_co",
            "reference_order_id": 92,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckfailedco": {
            "stage": "underwriting_co",
            "reference_order_id": 94,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckfailedpagoco": {
            "stage": "underwriting_pago_co",
            "reference_order_id": 95,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckfailedsantanderco": {
            "stage": "risk_evaluation_santander_co",
            "reference_order_id": 96,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckpassedco": {
            "stage": "underwriting_co",
            "reference_order_id": 98,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckpassedpagoco": {
            "stage": "underwriting_pago_co",
            "reference_order_id": 99,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckpsychometricfailedco": {
            "stage": "underwriting_psychometric_co",
            "reference_order_id": 101,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckpsychometricpassedco": {
            "stage": "underwriting_psychometric_co",
            "reference_order_id": 102,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckrejectedsantanderco": {
            "stage": "risk_evaluation_santander_co",
            "reference_order_id": 103,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "documentlistedinblacklistco": {
            "stage": "background_check_co",
            "reference_order_id": 105,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "documentsobtainedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 106,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "emailalreadylinkedtoexistingclientco": {
            "stage": "background_check_co",
            "reference_order_id": 108,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "emaillistedinblacklistco": {
            "stage": "background_check_co",
            "reference_order_id": 110,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "expeditedcheckoutloanproposalselectedco": {
            "stage": "expedited_loan_proposals_co",
            "reference_order_id": 111,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "failedtoobtainbureauinformationpagoco": {
            "stage": "underwriting_pago_co",
            "reference_order_id": 119,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "failedtoobtaincommercialinformationco": {
            "stage": "underwriting_co",
            "reference_order_id": 121,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "failedtoobtainscoringco": {
            "stage": "underwriting_co",
            "reference_order_id": 123,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "failedtoobtainscoringpagoco": {
            "stage": "underwriting_pago_co",
            "reference_order_id": 124,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "failedtosendsignedfilessantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 125,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "failedtosigndocumentssantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 126,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "failedtovalidateemailco": {
            "stage": "background_check_co",
            "reference_order_id": 127,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "firstpaymentdatechangedco": {
            "stage": "loan_proposals_co",
            "reference_order_id": 128,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "fraudcheckfailedco": {
            "stage": "fraud_check_co",
            "reference_order_id": 130,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "fraudcheckpassedco": {
            "stage": "fraud_check_co",
            "reference_order_id": 132,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "getclientinformationfailedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 133,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "getdocumentsfailedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 134,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosagentassigned": {
            "stage": "identity_photos",
            "reference_order_id": 135,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosapproved": {
            "stage": "identity_photos",
            "reference_order_id": 136,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotoscollected": {
            "stage": "identity_photos",
            "reference_order_id": 137,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosdiscarded": {
            "stage": "identity_photos",
            "reference_order_id": 138,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosdiscardedbyrisk": {
            "stage": "identity_photos",
            "reference_order_id": 139,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosevaluationstarted": {
            "stage": "identity_photos",
            "reference_order_id": 140,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotoskeptbyagent": {
            "stage": "identity_photos",
            "reference_order_id": 141,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosrejected": {
            "stage": "identity_photos",
            "reference_order_id": 142,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosstarted": {
            "stage": "identity_photos",
            "reference_order_id": 143,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyapproved": {
            "stage": "identity_photos",
            "reference_order_id": 144,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartydiscarded": {
            "stage": "identity_photos",
            "reference_order_id": 145,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartydiscardedbyrisk": {
            "stage": "identity_photos",
            "reference_order_id": 146,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartymanualverificationrequired": {
            "stage": "identity_photos",
            "reference_order_id": 147,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyrejected": {
            "stage": "identity_photos",
            "reference_order_id": 148,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartyrequested": {
            "stage": "identity_photos",
            "reference_order_id": 149,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identityphotosthirdpartystarted": {
            "stage": "identity_photos",
            "reference_order_id": 150,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywaagentassigned": {
            "stage": "identity_wa",
            "reference_order_id": 151,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywaapproved": {
            "stage": "identity_wa",
            "reference_order_id": 152,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywadiscarded": {
            "stage": "identity_wa",
            "reference_order_id": 153,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywadiscardedbyrisk": {
            "stage": "identity_wa",
            "reference_order_id": 154,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywainitialmessageresponsereceived": {
            "stage": "identity_wa",
            "reference_order_id": 155,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywainputinformationcompleted": {
            "stage": "identity_wa",
            "reference_order_id": 156,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywakeptbyagent": {
            "stage": "identity_wa",
            "reference_order_id": 157,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywarejected": {
            "stage": "identity_wa",
            "reference_order_id": 158,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "identitywastarted": {
            "stage": "identity_wa",
            "reference_order_id": 159,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "invalidemailco": {
            "stage": "background_check_co",
            "reference_order_id": 169,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "leaddisabledtooriginateco": {
            "stage": "preconditions_co",
            "reference_order_id": 170,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "leaddisabledtooriginatepagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 171,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "listedinofacco": {
            "stage": "background_check_co",
            "reference_order_id": 172,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptancecodedidnotmatchco": {
            "stage": "loan_acceptance_co",
            "reference_order_id": 174,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptancecodedidnotmatchsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 175,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptancenotificationfailedco": {
            "stage": "loan_acceptance_co",
            "reference_order_id": 177,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptancenotificationfailedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 178,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptancesentco": {
            "stage": "loan_acceptance_co",
            "reference_order_id": 180,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptancesentsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 181,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptancewasexpiredco": {
            "stage": "loan_acceptance_co",
            "reference_order_id": 183,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptancewasexpiredsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 184,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptedbygatewaysantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 187,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptedco": {
            "stage": "loan_acceptance_co",
            "reference_order_id": 188,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanacceptedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 189,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanproposalselectedco": {
            "stage": "loan_proposals_co",
            "reference_order_id": 193,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanproposalselectedsantanderco": {
            "stage": "loan_proposals_santander_co",
            "reference_order_id": 194,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "originationunexpectederroroccurred": {
            "stage": "GLOBAL",
            "reference_order_id": 197,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalapplicationcompletedco": {
            "stage": "preapproval_summary_co",
            "reference_order_id": 210,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalclienthasfootprintfromblacklistedentityco": {
            "stage": "underwriting_preapproval_co",
            "reference_order_id": 211,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalconditionalcreditcheckpassedco": {
            "stage": "underwriting_preapproval_co",
            "reference_order_id": 213,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalconditionalcreditcheckpassedpagoco": {
            "stage": "underwriting_preapproval_pago_co",
            "reference_order_id": 214,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalcreditcheckfailedco": {
            "stage": "underwriting_preapproval_co",
            "reference_order_id": 216,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalcreditcheckfailedpagoco": {
            "stage": "underwriting_preapproval_pago_co",
            "reference_order_id": 217,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalcreditcheckpassedco": {
            "stage": "underwriting_preapproval_co",
            "reference_order_id": 219,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalcreditcheckpassedpagoco": {
            "stage": "underwriting_preapproval_pago_co",
            "reference_order_id": 220,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalfailedtoobtainbureauinformationpagoco": {
            "stage": "underwriting_preapproval_pago_co",
            "reference_order_id": 221,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalfailedtoobtaincommercialinformationco": {
            "stage": "underwriting_preapproval_co",
            "reference_order_id": 223,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalfailedtoobtainscoringco": {
            "stage": "underwriting_preapproval_co",
            "reference_order_id": 225,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalfailedtoobtainscoringpagoco": {
            "stage": "underwriting_preapproval_pago_co",
            "reference_order_id": 226,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalloanproposalswithinvalidusuryratesco": {
            "stage": "underwriting_preapproval_co",
            "reference_order_id": 227,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalloanproposalswithinvalidusuryratespagoco": {
            "stage": "underwriting_preapproval_pago_co",
            "reference_order_id": 228,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preconditionswerevalidco": {
            "stage": "preconditions_co",
            "reference_order_id": 230,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preconditionswerevalidpagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 231,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "privacypolicyacceptedco": {
            "stage": "privacy_policy_co",
            "reference_order_id": 233,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "privacypolicyacceptedsantanderco": {
            "stage": "privacy_policy_santander_co",
            "reference_order_id": 234,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "prospectalreadylinkedtodifferentcellphoneco": {
            "stage": "background_check_co",
            "reference_order_id": 236,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "prospectdocumentnotfoundinbureauco": {
            "stage": "basic_identity_co",
            "reference_order_id": 238,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "prospectinfonotcompleteinbureauco": {
            "stage": "basic_identity_co",
            "reference_order_id": 240,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationapproved": {
            "stage": "psychometric_assessment",
            "reference_order_id": 241,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationfailed": {
            "stage": "psychometric_assessment",
            "reference_order_id": 242,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationisrequiredco": {
            "stage": "underwriting_co",
            "reference_order_id": 244,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationisrequiredpagoco": {
            "stage": "underwriting_pago_co",
            "reference_order_id": 245,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationrated": {
            "stage": "psychometric_assessment",
            "reference_order_id": 246,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationrejected": {
            "stage": "psychometric_assessment",
            "reference_order_id": 247,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "psychometricevaluationstarted": {
            "stage": "psychometric_assessment",
            "reference_order_id": 248,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "requestedamountwasgreaterthanmaximumconfiguredco": {
            "stage": "preconditions_co",
            "reference_order_id": 250,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "requestedamountwasgreaterthanmaximumconfiguredpagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 251,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "requestedamountwaslessthanminimumconfiguredco": {
            "stage": "preconditions_co",
            "reference_order_id": 253,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "requestedamountwaslessthanminimumconfiguredpagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 254,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "sendloanacceptancemaxattemptsreachedco": {
            "stage": "loan_acceptance_co",
            "reference_order_id": 257,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "sendloanacceptancemaxattemptsreachedsantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 258,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "signeddocumentssantanderco": {
            "stage": "loan_acceptance_santander_co",
            "reference_order_id": 259,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "workinformationreceivedsantanderco": {
            "stage": "work_information_santander_co",
            "reference_order_id": 261,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientpendingapplicationfoundco": {
            "stage": "preconditions_co",
            "reference_order_id": 262,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientpendingapplicationfoundpagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 263,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "creditcheckpassedwithoutloan": {
            "stage": "underwriting",
            "reference_order_id": 264,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "returningclientjourneyisdisableco": {
            "stage": "preconditions_co",
            "reference_order_id": 265,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidatedco": {
            "stage": "cellphone_validation_co",
            "reference_order_id": 266,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationcodedidnotmatchco": {
            "stage": "cellphone_validation_co",
            "reference_order_id": 267,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationcodewasexpiredco": {
            "stage": "cellphone_validation_co",
            "reference_order_id": 268,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationnotificationsentco": {
            "stage": "cellphone_validation_co",
            "reference_order_id": 269,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "personalinformationupdatedco": {
            "stage": "personal_information_co",
            "reference_order_id": 270,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationclosedbynewone": {
            "stage": "GLOBAL",
            "reference_order_id": 271,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationmaxattemptsreachedco": {
            "stage": "cellphone_validation_co",
            "reference_order_id": 272,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientloanproposalswerewithinvalidusuryratepagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 273,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "sendcellphonevalidationnotificationmaxattemptsreachedco": {
            "stage": "cellphone_validation_co",
            "reference_order_id": 274,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanproposalswithinvalidusuryratesco": {
            "stage": "underwriting_co",
            "reference_order_id": 275,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "clientinsufficientaddicupopagoco": {
            "stage": "preconditions_pago_co",
            "reference_order_id": 276,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "emailverificationpassed": {
            "stage": "email_verification",
            "reference_order_id": 277,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "emailverificationsent": {
            "stage": "email_verification",
            "reference_order_id": 278,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "firstpaymentdatercchangedco": {
            "stage": "loan_proposals_co",
            "reference_order_id": 279,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "emailverificationtokenwasinvalid": {
            "stage": "email_verification",
            "reference_order_id": 280,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "conditionalclientcreditcheckpassedpagoco": {
            "stage": "underwriting_rc_pago_co",
            "reference_order_id": 281,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "faceverificationrejected": {
            "stage": "face_verification",
            "reference_order_id": 270,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "facephotocollected": {
            "stage": "face_verification",
            "reference_order_id": 271,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "faceverificationapproved": {
            "stage": "face_verification",
            "reference_order_id": 282,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "manualidentityverificationrequired": {
            "stage": "face_verification",
            "reference_order_id": 283,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoicedeclinedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 270,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoiceurlnotobtainedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 271,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoiceurlobtainedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 272,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoiceconfirmedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 273,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoicerequestedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 274,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoiceupdatedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 275,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "bnpnpaymentapprovedco": {
            "stage": "bnpn_summary_co",
            "reference_order_id": 276,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoicecreatedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 275,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "sendemailverificationmaxattemptsreached": {
            "stage": "refinance_loan_proposals_co",
            "reference_order_id": 284,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preconditionsrefinancewerevalidpagoco": {
            "stage": "refinance_loan_proposals_co",
            "reference_order_id": 285,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoicefailedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 292,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "pseinvoicerequestfailedco": {
            "stage": "pse_payment_co",
            "reference_order_id": 293,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "failedtocreateloanrefinanceproposalsco": {
            "stage": "pse_payment_co",
            "reference_order_id": 288,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "loanrefinanceproposalscreatedco": {
            "stage": "refinance_loan_proposals_co",
            "reference_order_id": 289,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "refinanceloanproposalselectedco": {
            "stage": "email_verification",
            "reference_order_id": 290,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "refinanceloanacceptedco": {
            "stage": "refinance_loan_acceptance_co",
            "reference_order_id": 291,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "applicationclosedbynewproductselection": {
            "stage": "GLOBAL",
            "reference_order_id": 292,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        },
        "preapprovalsurveycompleted": {
            "stage": "GLOBAL",
            "reference_order_id": 293,
            "direct_attributes": [
                "application_id",
                "client_id",
                "journey_name",
                "event_name",
                "ocurred_on",
                "journey_stage_name",
                "event_type",
                "ally_slug",
                "product",
                "client_type",
                "channel"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "channel",
            "client_id",
            "client_type",
            "event_name",
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
