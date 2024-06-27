{% macro return_config_br_f_applications_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_applications_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-14 15:30 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_applications_br",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "allyisdisabledtooriginatebr": {
            "direct_attributes": [
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
        "applicationcreated": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_date",
                "channel",
                "client_checkout_type",
                "client_id",
                "client_type",
                "custom_platform_version",
                "journey_name",
                "order_id",
                "order_type",
                "product",
                "requested_amount",
                "requested_amount_without_discount",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "backgroundcheckpassedbr": {
            "direct_attributes": [
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
        "checkoutloginsentbr": {
            "direct_attributes": [
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
        "clientloanapplicationcreated": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_date",
                "application_channel_legacy",
                "client_id",
                "custom_is_returning_client_legacy",
                "custom_platform_version",
                "requested_amount",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "clientpreapprovaljourneyisdisabledbr": {
            "direct_attributes": [
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
        "clientwaspreapprovedbeforebr": {
            "direct_attributes": [
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
        "loanproposalselectedbr": {
            "direct_attributes": [
                "application_id",
                "campaign_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pixpaymentrequestedbr": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "channel",
                "client_id",
                "client_type",
                "custom_is_bnpn_branched",
                "journey_name",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preapprovalapplicationcompletedbr": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "channel",
                "client_id",
                "client_type",
                "custom_is_preapproval_completed",
                "journey_name",
                "preapproval_amount",
                "preapproval_expiration_date",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "preconditionswerevalidbr": {
            "direct_attributes": [
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
        "privacypolicyaccepted": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_channel_legacy",
                "client_id",
                "custom_is_privacy_policy_accepted",
                "requested_amount",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "privacypolicyacceptedbr": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "channel",
                "campaign_id",
                "client_id",
                "client_type",
                "custom_is_privacy_policy_accepted",
                "journey_name",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "privacypolicysent": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "client_id",
                "custom_platform_version",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectloanapplicationcreatedfromcheckpoint": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_date",
                "application_channel_legacy",
                "client_id",
                "custom_is_checkpoint_application_legacy",
                "custom_platform_version",
                "requested_amount",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectloanapplicationcreatedv2": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_date",
                "application_channel_legacy",
                "client_id",
                "custom_platform_version",
                "order_id",
                "requested_amount",
                "store_slug",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectupgradedtoclient": {
            "direct_attributes": [
                "application_id",
                "store_user_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "requestedamountwasgreaterthanmaximumconfiguredbr": {
            "direct_attributes": [
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
        "requestedamountwaslessthanminimumconfiguredbr": {
            "direct_attributes": [
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
        "backgroundcheckpassed": {
            "direct_attributes": [
                "application_id",
                "store_user_id",
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
            "client_checkout_type",
            "client_id",
            "client_type",
            "custom_is_bnpn_branched",
            "custom_is_checkpoint_application_legacy",
            "custom_is_preapproval_completed",
            "custom_is_privacy_policy_accepted",
            "custom_is_returning_client_legacy",
            "custom_platform_version",
            "journey_name",
            "ocurred_on",
            "order_id",
            "order_type",
            "preapproval_amount",
            "preapproval_expiration_date",
            "product",
            "requested_amount",
            "requested_amount_without_discount",
            "store_slug",
            "store_user_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}