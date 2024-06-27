{% macro return_config_co_f_applications_order_details_v2_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_order_details_v2_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2023-11-13 14:40 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_order_details_v2_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "applicationcreated": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "store_slug",
                "channel",
                "custom_platform_version",
                "order_id",
                "order_currency",
                "order_description",
                "order_shipping_amount",
                "order_taxes_total_amount",
                "order_total_amount",
                "order_total_without_discount_amount",
                "order_type",
                "order_geolocation_latitude",
                "order_geolocation_longitude",
                "order_billing_address_city",
                "order_billing_address_complement",
                "order_billing_address_country",
                "order_billing_address_line_one",
                "order_billing_address_neighborhood",
                "order_billing_address_number",
                "order_billing_address_postal_code",
                "order_billing_address_street",
                "order_billing_address_sub_country",
                "order_pick_up_address_city",
                "order_pick_up_address_country",
                "order_pick_up_address_line_one",
                "order_shipping_address_city",
                "order_shipping_address_complement",
                "order_shipping_address_country",
                "order_shipping_address_line_one",
                "order_shipping_address_neighborhood",
                "order_shipping_address_number",
                "order_shipping_address_postal_code",
                "order_shipping_address_street",
                "order_shipping_address_sub_country",
                "order_shopping_intent_attributable",
                "order_shopping_intent_id",
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
            "order_billing_address_city",
            "order_billing_address_complement",
            "order_billing_address_country",
            "order_billing_address_line_one",
            "order_billing_address_neighborhood",
            "order_billing_address_number",
            "order_billing_address_postal_code",
            "order_billing_address_street",
            "order_billing_address_sub_country",
            "order_currency",
            "order_description",
            "order_geolocation_latitude",
            "order_geolocation_longitude",
            "order_id",
            "order_pick_up_address_city",
            "order_pick_up_address_country",
            "order_pick_up_address_line_one",
            "order_shipping_address_city",
            "order_shipping_address_complement",
            "order_shipping_address_country",
            "order_shipping_address_line_one",
            "order_shipping_address_neighborhood",
            "order_shipping_address_number",
            "order_shipping_address_postal_code",
            "order_shipping_address_street",
            "order_shipping_address_sub_country",
            "order_shipping_amount",
            "order_shopping_intent_attributable",
            "order_shopping_intent_id",
            "order_taxes_total_amount",
            "order_total_amount",
            "order_total_without_discount_amount",
            "order_type",
            "store_slug"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}