{% macro return_config_co_f_applications_marketplace_allies_product_policies_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_marketplace_allies_product_policies_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-03-07 17:07 TZ-0500",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_marketplace_allies_product_policies_co",
        "files_db_table_pks": [
            "custom_application_suborder_product_policy_pairing_id"
        ]
    },
    "events": {
        "bnpnpaymentapprovedco_unnested_by_marketplace_suborders_product_policies": {
            "direct_attributes": [
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "order_id",
                "product",
                "suborder_id",
                "suborder_ally_slug",
                "product_policy_type",
                "product_policy_max_amount",
                "product_policy_origination_mdf",
                "product_policy_cancellation_mdf",
                "product_policy_fraud_mdf",
                "custom_event_suborder_pairing_id",
                "custom_application_suborder_pairing_id",
                "custom_application_suborder_product_policy_pairing_id"
            ],
            "custom_attributes": {}
        },
        "loanproposalselectedco_unnested_by_marketplace_suborders_product_policies": {
            "direct_attributes": [
                "ocurred_on",
                "application_id",
                "client_id",
                "ally_slug",
                "order_id",
                "product",
                "suborder_id",
                "suborder_ally_slug",
                "product_policy_type",
                "product_policy_max_amount",
                "product_policy_origination_mdf",
                "product_policy_cancellation_mdf",
                "product_policy_fraud_mdf",
                "custom_event_suborder_pairing_id",
                "custom_application_suborder_pairing_id",
                "custom_application_suborder_product_policy_pairing_id"
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
            "custom_application_suborder_product_policy_pairing_id",
            "custom_event_suborder_pairing_id",
            "ocurred_on",
            "order_id",
            "product",
            "product_policy_cancellation_mdf",
            "product_policy_fraud_mdf",
            "product_policy_max_amount",
            "product_policy_origination_mdf",
            "product_policy_type",
            "suborder_ally_slug",
            "suborder_id"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}