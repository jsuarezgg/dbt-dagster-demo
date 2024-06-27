{% macro return_config_co_f_ally_pop_material_shipping_orders_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_ally_pop_material_shipping_orders_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-12-01 17:31 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_ally_pop_material_shipping_orders_co",
        "files_db_table_pks": [
            "shipping_order_id"
        ]
    },
    "events": {
        "allypopmaterialshippingordercreated": {
            "direct_attributes": [
                "shipping_order_id",
                "shipping_order_number",
                "shipping_method",
                "ally_slug",
                "store_slug",
                "shipping_order_created_at",
                "buyer_email",
                "buyer_full_name",
                "buyer_phone_number",
                "delivery_address",
                "delivery_address_reference",
                "delivery_city_code",
                "delivery_city_name",
                "delivery_recipient_full_name",
                "delivery_recipient_phone_number",
                "delivery_region_code",
                "delivery_country_code",
                "delivery_country_name",
                "delivery_region_name",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "buyer_email",
            "buyer_full_name",
            "buyer_phone_number",
            "delivery_address",
            "delivery_address_reference",
            "delivery_city_code",
            "delivery_city_name",
            "delivery_country_code",
            "delivery_country_name",
            "delivery_recipient_full_name",
            "delivery_recipient_phone_number",
            "delivery_region_code",
            "delivery_region_name",
            "ocurred_on",
            "shipping_method",
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