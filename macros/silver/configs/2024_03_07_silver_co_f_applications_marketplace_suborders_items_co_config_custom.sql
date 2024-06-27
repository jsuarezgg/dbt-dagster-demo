{% macro return_config_co_f_applications_marketplace_suborders_items_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_marketplace_suborders_items_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-03-07 17:07 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_marketplace_suborders_items_co",
        "files_db_table_pks": [
            "custom_application_suborder_sku_pairing_id"
        ]
    },
    "events": {
        "applicationcreated_unnested_by_marketplace_suborders_items": {
            "direct_attributes": [
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "order_id",
                "product",
                "suborder_id",
                "suborder_ally_slug",
                "custom_suborder_item_idx",
                "suborder_item_sku",
                "suborder_item_name",
                "suborder_item_unit_price",
                "suborder_item_quantity",
                "suborder_item_discount",
                "suborder_item_seller_id",
                "custom_event_suborder_pairing_id",
                "custom_application_suborder_pairing_id",
                "custom_application_suborder_sku_pairing_id"
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
            "custom_application_suborder_sku_pairing_id",
            "custom_event_suborder_pairing_id",
            "custom_suborder_item_idx",
            "ocurred_on",
            "order_id",
            "product",
            "suborder_ally_slug",
            "suborder_id",
            "suborder_item_discount",
            "suborder_item_name",
            "suborder_item_quantity",
            "suborder_item_seller_id",
            "suborder_item_sku",
            "suborder_item_unit_price"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}