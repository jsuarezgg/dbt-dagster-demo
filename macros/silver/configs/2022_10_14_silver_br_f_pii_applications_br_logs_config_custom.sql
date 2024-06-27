{% macro return_config_br_f_pii_applications_br_logs_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_pii_applications_br_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2022-09-15 09:31 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_pii_applications_br_logs",
        "files_db_table_pks": [
            "event_id"
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
                "event_id",
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
                "application_cellphone",
                "application_date",
                "application_email",
                "channel",
                "client_checkout_type",
                "client_id",
                "client_type",
                "custom_platform_version",
                "event_id",
                "id_number",
                "id_type",
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
                "application_cellphone",
                "application_email",
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
        "checkoutloginsentbr": {
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
                "application_cellphone",
                "application_date",
                "application_email",
                "application_channel_legacy",
                "client_id",
                "custom_is_returning_client_legacy",
                "custom_platform_version",
                "event_id",
                "id_number",
                "id_type",
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
                "event_id",
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
                "event_id",
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
                "event_id",
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
                "event_id",
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
                "event_id",
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
                "application_cellphone",
                "application_email",
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
        "privacypolicyaccepted": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_cellphone",
                "application_email",
                "application_channel_legacy",
                "client_id",
                "event_id",
                "id_number",
                "id_type",
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
                "application_cellphone",
                "application_email",
                "channel",
                "campaign_id",
                "client_id",
                "client_type",
                "custom_is_privacy_policy_accepted",
                "event_id",
                "journey_name",
                "id_number",
                "id_type",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "privacypolicysent": {
            "direct_attributes": [
                "application_id",
                "ally_slug",
                "application_cellphone",
                "client_id",
                "custom_platform_version",
                "event_id",
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
                "application_cellphone",
                "application_date",
                "application_email",
                "application_channel_legacy",
                "client_id",
                "custom_is_checkpoint_application_legacy",
                "custom_platform_version",
                "event_id",
                "id_number",
                "id_type",
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
                "event_id",
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
                "application_email",
                "event_id",
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
                "event_id",
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
                "event_id",
                "journey_name",
                "product",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "backgroundcheckmaxattemptsreachedbr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonealreadylinkedtoadifferentprospectbr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonealreadylinkedtoexistingclientbr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "cellphonelistedinblacklistbr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "documentlistedinblacklistbr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "emailalreadylinkedtoexistingclientbr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "emaillistedinblacklistbr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "invalidemailbr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "prospectalreadylinkedtodifferentcellphonebr": {
            "direct_attributes": [
                "application_id",
                "application_cellphone",
                "application_email",
                "event_id",
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
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_cellphone",
            "application_channel_legacy",
            "application_date",
            "application_email",
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
            "event_id",
            "id_number",
            "id_type",
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