{% macro return_config_co_f_ally_pop_material_shipping_orders_items_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_ally_pop_material_shipping_orders_items_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-12-01 17:31 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_ally_pop_material_shipping_orders_items_co",
        "files_db_table_pks": [
            "custom_item_id"
        ]
    },
    "events": {
        "allypopmaterialshippingordercreated_unnested_by_item": {
            "direct_attributes": [
                "custom_item_id",
                "shipping_order_id",
                "shipping_order_number",
                "ally_slug",
                "store_slug",
                "shipping_order_created_at",
                "item_quantity",
                "item_sku",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "custom_item_id",
            "item_quantity",
            "item_sku",
            "ocurred_on",
            "shipping_order_created_at",
            "shipping_order_id",
            "shipping_order_number",
            "store_slug"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}