{% macro return_config_co_f_transaction_cancellations_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_transaction_cancellations_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-04-08 14:43 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_transaction_cancellations_co_logs",
        "files_db_table_pks": [
            "event_id"
        ]
    },
    "events": {
        "allytransactioncanceled": {
            "direct_attributes": [
                "event_id",
                "transaction_id",
                "ally_slug",
                "id_number",
                "loan_id",
                "client_id",
                "user_id",
                "user_role",
                "store_user_name",
                "transaction_cancellation_date",
                "transaction_cancellation_reason",
                "transaction_cancellation_amount",
                "transaction_cancellation_id",
                "transaction_cancellation_source",
                "transaction_cancellation_type",
                "transaction_subproduct",
                "custom_event_domain",
                "custom_transaction_cancellation_status",
                "marketplace_channel",
                "marketplace_suborder_ally",
                "custom_transaction_marketplace_suborder_ally_pairing_id",
                "custom_transaction_marketplace_channel_pairing_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "transactioncancellationrequested": {
            "direct_attributes": [
                "event_id",
                "transaction_id",
                "ally_slug",
                "id_number",
                "loan_id",
                "client_id",
                "loan_source",
                "store_user_name",
                "user_id",
                "user_role",
                "transaction_subproduct",
                "transaction_cancellation_amount",
                "transaction_cancellation_date",
                "transaction_cancellation_reason",
                "transaction_cancellation_source",
                "transaction_cancellation_type",
                "custom_event_domain",
                "custom_transaction_cancellation_status",
                "marketplace_channel",
                "marketplace_suborder_ally",
                "custom_transaction_marketplace_suborder_ally_pairing_id",
                "custom_transaction_marketplace_channel_pairing_id",
                "transaction_cancellation_channel",
                "transaction_cancellation_id",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "transactionrequestedtocancelbyid": {
            "direct_attributes": [
                "event_id",
                "transaction_id",
                "ally_slug",
                "transaction_cancellation_amount",
                "transaction_cancellation_date",
                "transaction_cancellation_reason",
                "transaction_cancellation_source",
                "transaction_cancellation_type",
                "custom_event_domain",
                "custom_transaction_cancellation_status",
                "marketplace_channel",
                "marketplace_suborder_ally",
                "custom_transaction_marketplace_suborder_ally_pairing_id",
                "custom_transaction_marketplace_channel_pairing_id",
                "transaction_cancellation_channel",
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
            "custom_transaction_cancellation_status",
            "custom_transaction_marketplace_channel_pairing_id",
            "custom_transaction_marketplace_suborder_ally_pairing_id",
            "event_id",
            "id_number",
            "loan_id",
            "loan_source",
            "marketplace_channel",
            "marketplace_suborder_ally",
            "ocurred_on",
            "store_user_name",
            "transaction_cancellation_amount",
            "transaction_cancellation_channel",
            "transaction_cancellation_date",
            "transaction_cancellation_id",
            "transaction_cancellation_reason",
            "transaction_cancellation_source",
            "transaction_cancellation_type",
            "transaction_id",
            "transaction_subproduct",
            "user_id",
            "user_role"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}