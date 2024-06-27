{% macro return_config_co_f_applications_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-03-07 17:16 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "allyapplicationupdated": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "event_id",
                "client_id",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "allyisdisabledtooriginateco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "allyisdisabledtooriginatepagoco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationcreated": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "suborders_ally_slug_array",
                "application_date",
                "event_id",
                "channel",
                "client_id",
                "client_is_transactional_based",
                "client_type",
                "custom_platform_version",
                "order_id",
                "product",
                "requested_amount",
                "requested_amount_without_discount",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "backgroundcheckpassedco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "channel",
                "client_id",
                "client_type",
                "event_id",
                "journey_name",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "checkoutloginsentco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "channel",
                "client_id",
                "client_type",
                "event_id",
                "journey_name",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientloanapplicationcreated": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_date",
                "event_id",
                "client_id",
                "custom_is_returning_client_legacy",
                "custom_platform_version",
                "requested_amount",
                "requested_amount_without_discount",
                "store_slug",
                "store_user_id",
                "store_user_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientpreapprovaljourneyisdisabledpagoco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientwaspreapprovedbeforepagoco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "creditcheckapprovedsantanderco": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "custom_is_santander_branched",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "leaddisabledtooriginateco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "leaddisabledtooriginatepagoco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanproposalselectedco": {
            "direct_attributes": [
                "application_id",
                "campaign_id",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loanproposalselectedsantanderco": {
            "direct_attributes": [
                "application_id",
                "campaign_id",
                "event_id",
                "custom_is_santander_branched",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preapprovalapplicationcompletedco": {
            "direct_attributes": [
                "application_id",
                "custom_is_preapproval_completed",
                "event_id",
                "preapproval_amount",
                "preapproval_expiration_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preconditionswerevalidco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preconditionsrefinancewerevalidpagoco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preconditionswerevalidpagoco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "privacypolicyaccepted": {
            "direct_attributes": [
                "application_id",
                "application_date",
                "event_id",
                "client_id",
                "custom_is_privacy_policy_accepted",
                "requested_amount",
                "store_slug",
                "store_user_id",
                "store_user_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "privacypolicyacceptedco": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "campaign_id",
                "client_id",
                "custom_is_privacy_policy_accepted",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "privacypolicyacceptedsantanderco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "event_id",
                "campaign_id",
                "client_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "privacypolicysent": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_date",
                "event_id",
                "client_id",
                "custom_platform_version",
                "store_slug",
                "store_user_id",
                "store_user_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectapplicationpreapproved": {
            "direct_attributes": [
                "application_id",
                "custom_is_preapproval_completed",
                "event_id",
                "preapproval_amount",
                "preapproval_expiration_date",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectloanapplicationcreatedfromcheckpoint": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_channel_legacy",
                "application_date",
                "event_id",
                "client_id",
                "custom_is_checkpoint_application_legacy",
                "custom_is_preapproval_completed",
                "custom_platform_version",
                "requested_amount",
                "requested_amount_without_discount",
                "store_slug",
                "store_user_id",
                "store_user_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectloanapplicationcreatedv2": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_channel_legacy",
                "application_date",
                "event_id",
                "client_id",
                "custom_platform_version",
                "order_id",
                "requested_amount",
                "requested_amount_without_discount",
                "store_slug",
                "store_user_id",
                "store_user_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectupgradedtoclient": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "requestedamountwasgreaterthanmaximumconfiguredco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "requestedamountwasgreaterthanmaximumconfiguredpagoco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "requestedamountwaslessthanminimumconfiguredco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "requestedamountwaslessthanminimumconfiguredpagoco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "returningclientjourneyisdisableco": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonevalidationnotificationsentco": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "checkoutloginacceptancenotificationfailedco": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientaddicupowasbalancezeropagoco": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientinsufficientaddicupopagoco": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientpendingapplicationfoundco": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientpendingapplicationfoundpagoco": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "journey_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "backgroundcheckpassed": {
            "direct_attributes": [
                "application_id",
                "event_id",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoicerequestedco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "ally_slug",
                "channel",
                "client_id",
                "client_type",
                "journey_name",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoicerequestfailedco": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "ally_slug",
                "channel",
                "client_id",
                "client_type",
                "journey_name",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "applicationclosedbynewproductselection": {
            "direct_attributes": [
                "event_id",
                "application_id",
                "ally_slug",
                "channel",
                "client_id",
                "client_type",
                "journey_name",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_channel_legacy",
            "application_date",
            "application_id",
            "campaign_id",
            "channel",
            "client_id",
            "client_is_transactional_based",
            "client_type",
            "custom_is_checkpoint_application_legacy",
            "custom_is_preapproval_completed",
            "custom_is_privacy_policy_accepted",
            "custom_is_returning_client_legacy",
            "custom_is_santander_branched",
            "custom_platform_version",
            "event_id",
            "journey_name",
            "ocurred_on",
            "order_id",
            "preapproval_amount",
            "preapproval_expiration_date",
            "product",
            "requested_amount",
            "requested_amount_without_discount",
            "store_slug",
            "store_user_id",
            "store_user_name",
            "suborders_ally_slug_array"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}