{% macro return_config_co_f_loan_cancellations_v2_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_loan_cancellations_v2_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-04-08 14:43 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_loan_cancellations_v2_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "loancancellationorderprocessed": {
            "direct_attributes": [
                "event_id",
                "loan_id",
                "ally_slug",
                "client_id",
                "user_id",
                "loan_cancellation_id",
                "loan_cancellation_reason",
                "loan_cancellation_order_date",
                "loan_approved_amount",
                "loan_origination_date",
                "custom_event_domain",
                "custom_loan_cancellation_status",
                "custom_loan_marketplace_suborder_ally_pairing_id",
                "custom_loan_marketplace_channel_pairing_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loancancellationorderprocessedv2": {
            "direct_attributes": [
                "event_id",
                "loan_id",
                "ally_slug",
                "client_id",
                "user_id",
                "loan_cancellation_id",
                "loan_cancellation_amount",
                "loan_cancellation_type",
                "loan_cancellation_reason",
                "loan_cancellation_order_date",
                "loan_approved_amount",
                "loan_origination_date",
                "user_role",
                "store_user_name",
                "custom_event_domain",
                "custom_loan_cancellation_status",
                "marketplace_channel",
                "marketplace_suborder_ally",
                "custom_loan_marketplace_suborder_ally_pairing_id",
                "custom_loan_marketplace_channel_pairing_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "loancancellationorderannulledv2": {
            "direct_attributes": [
                "event_id",
                "loan_id",
                "client_id",
                "user_id",
                "loan_cancellation_annulment_reason",
                "loan_cancellation_id",
                "custom_event_domain",
                "custom_loan_cancellation_status",
                "marketplace_channel",
                "marketplace_suborder_ally",
                "custom_loan_marketplace_suborder_ally_pairing_id",
                "custom_loan_marketplace_channel_pairing_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "client_id",
            "custom_event_domain",
            "custom_loan_cancellation_status",
            "custom_loan_marketplace_channel_pairing_id",
            "custom_loan_marketplace_suborder_ally_pairing_id",
            "event_id",
            "loan_approved_amount",
            "loan_cancellation_amount",
            "loan_cancellation_annulment_reason",
            "loan_cancellation_id",
            "loan_cancellation_order_date",
            "loan_cancellation_reason",
            "loan_cancellation_type",
            "loan_id",
            "loan_origination_date",
            "marketplace_channel",
            "marketplace_suborder_ally",
            "ocurred_on",
            "store_user_name",
            "user_id",
            "user_role"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}