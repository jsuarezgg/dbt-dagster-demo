{% macro return_config_co_f_applications_marketplace_suborders_co_logs_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_marketplace_suborders_co_logs-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-03-13 12:15 TZ-0500",
    "is_group_feature_active": false,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_marketplace_suborders_co_logs",
        "files_db_table_pks": [
            "custom_event_suborder_pairing_id"
        ]
    },
    "events": {
        "applicationcreated_unnested_by_marketplace_suborders": {
            "direct_attributes": [
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "order_id",
                "product",
                "suborder_id",
                "suborder_total_amount",
                "suborder_total_amount_without_discount",
                "suborder_shipping_amount",
                "suborder_attribution_weight_by_number_of_allies",
                "suborder_attribution_weight_by_total_amount",
                "suborder_attribution_weight_by_total_without_discount_amount",
                "suborder_vtex_external_id_array",
                "suborder_ally_slug",
                "suborder_vtex_seller_id_array",
                "suborder_store_slug",
                "suborder_marketplace_purchase_fee",
                "custom_event_suborder_pairing_id",
                "custom_application_suborder_pairing_id"
            ],
            "custom_attributes": {}
        },
        "bnpnpaymentapprovedco_unnested_by_marketplace_suborders": {
            "direct_attributes": [
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "order_id",
                "product",
                "suborder_id",
                "suborder_total_amount",
                "suborder_ally_slug",
                "suborder_store_slug",
                "custom_event_suborder_pairing_id",
                "custom_application_suborder_pairing_id"
            ],
            "custom_attributes": {}
        },
        "loanproposalselectedco_unnested_by_marketplace_suborders": {
            "direct_attributes": [
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "order_id",
                "product",
                "suborder_id",
                "suborder_total_amount",
                "suborder_ally_slug",
                "suborder_store_slug",
                "custom_event_suborder_pairing_id",
                "custom_application_suborder_pairing_id"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "client_id",
            "custom_application_suborder_pairing_id",
            "custom_event_suborder_pairing_id",
            "ocurred_on",
            "order_id",
            "product",
            "suborder_ally_slug",
            "suborder_attribution_weight_by_number_of_allies",
            "suborder_attribution_weight_by_total_amount",
            "suborder_attribution_weight_by_total_without_discount_amount",
            "suborder_id",
            "suborder_marketplace_purchase_fee",
            "suborder_shipping_amount",
            "suborder_store_slug",
            "suborder_total_amount",
            "suborder_total_amount_without_discount",
            "suborder_vtex_external_id_array",
            "suborder_vtex_seller_id_array"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}