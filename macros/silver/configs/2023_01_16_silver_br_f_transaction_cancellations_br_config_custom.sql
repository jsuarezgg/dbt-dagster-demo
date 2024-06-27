{% macro return_config_br_f_transaction_cancellations_br_custom() %}
{#-target_country=br;target_schema=silver;target_table_name=f_transaction_cancellations_br-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-01-18 12:01 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "br",
        "files_db_table_name": "f_transaction_cancellations_br",
        "files_db_table_pks": [
            "transaction_id"
        ]
    },
    "events": {
        "allytransactioncanceled": {
            "direct_attributes": [
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
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "transactioncancellationrequested": {
            "direct_attributes": [
                "transaction_id",
                "ally_slug",
                "id_number",
                "loan_id",
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
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "transactionrequestedtocancelbyid": {
            "direct_attributes": [
                "transaction_id",
                "transaction_cancellation_amount",
                "transaction_cancellation_date",
                "transaction_cancellation_reason",
                "transaction_cancellation_source",
                "transaction_cancellation_type",
                "custom_event_domain",
                "custom_transaction_cancellation_status",
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
            "id_number",
            "loan_id",
            "loan_source",
            "ocurred_on",
            "store_user_name",
            "transaction_cancellation_amount",
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