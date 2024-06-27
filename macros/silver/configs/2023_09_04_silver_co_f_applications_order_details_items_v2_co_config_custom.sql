{% macro return_config_co_f_applications_order_details_items_v2_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_order_details_items_v2_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-09-05 11:25 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_order_details_items_v2_co",
        "files_db_table_pks": [
            "surrogate_key"
        ]
    },
    "events": {
        "applicationcreated_unnested_by_items": {
            "direct_attributes": [
                "surrogate_key",
                "application_id",
                "client_id",
                "ally_slug",
                "store_slug",
                "channel",
                "custom_platform_version",
                "order_id",
                "order_item_row_number_by_item_name",
                "order_item_name",
                "order_item_category",
                "order_item_discount",
                "order_item_picture_url",
                "order_item_quantity",
                "order_item_sku",
                "order_item_tax_amount",
                "order_item_unit_price",
                "ocurred_on"
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
            "custom_platform_version",
            "ocurred_on",
            "order_id",
            "order_item_category",
            "order_item_discount",
            "order_item_name",
            "order_item_picture_url",
            "order_item_quantity",
            "order_item_row_number_by_item_name",
            "order_item_sku",
            "order_item_tax_amount",
            "order_item_unit_price",
            "store_slug",
            "surrogate_key"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}