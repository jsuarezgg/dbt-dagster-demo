{% macro return_config_co_f_applications_pse_payment_co_custom() %}
{#-target_country=co;target_schema=silver;target_table_name=f_applications_pse_payment_co-#}
{%- set configuration_dict = {
    "config_parser_execution_date": "2024-03-12 11:58 TZ-0300",
    "is_group_feature_active": true,
    "relevant_properties": {
        "schema_country": "co",
        "files_db_table_name": "f_applications_pse_payment_co",
        "files_db_table_pks": [
            "application_id"
        ]
    },
    "events": {
        "pseinvoiceconfirmedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_amount",
                "payment_created_at",
                "payment_finalized_at",
                "payment_financial_institution_code",
                "payment_financial_institution_name",
                "payer_email",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "payment_provider",
                "payment_reference",
                "payment_status",
                "payment_status_reason",
                "payment_transaction_id",
                "payment_trazability_code",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoicecreatedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_amount",
                "payment_created_at",
                "payment_finalized_at",
                "payment_financial_institution_code",
                "payment_financial_institution_name",
                "payer_cellphone_country_code",
                "payer_cellphone_number",
                "payer_email",
                "payer_full_name",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "payment_provider",
                "payment_reference",
                "payment_status",
                "payment_status_reason",
                "payment_transaction_id",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoicedeclinedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_amount",
                "payment_created_at",
                "payment_finalized_at",
                "payment_financial_institution_code",
                "payment_financial_institution_name",
                "payer_cellphone_country_code",
                "payer_cellphone_number",
                "payer_email",
                "payer_full_name",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "payment_provider",
                "payment_reference",
                "payment_status",
                "payment_status_reason",
                "payment_transaction_id",
                "payment_trazability_code",
                "payment_rejection_code",
                "payment_rejection_description",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoicefailedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_amount",
                "payment_created_at",
                "payment_finalized_at",
                "payment_financial_institution_code",
                "payment_financial_institution_name",
                "payer_cellphone_country_code",
                "payer_cellphone_number",
                "payer_email",
                "payer_full_name",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "payment_provider",
                "payment_reference",
                "payment_status",
                "payment_status_reason",
                "payment_transaction_id",
                "payment_trazability_code",
                "payment_rejection_code",
                "payment_rejection_description",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoicerequestedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_amount",
                "payment_created_at",
                "payment_financial_institution_code",
                "payment_financial_institution_name",
                "payer_cellphone_country_code",
                "payer_cellphone_number",
                "payer_email",
                "payer_full_name",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "payment_provider",
                "payment_reference",
                "payment_status",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoicerequestfailedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_created_at",
                "payment_financial_institution_code",
                "payer_cellphone_country_code",
                "payer_cellphone_number",
                "payer_email",
                "payer_full_name",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoiceupdatedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_amount",
                "payment_created_at",
                "payment_finalized_at",
                "payment_financial_institution_code",
                "payment_financial_institution_name",
                "payer_email",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "payment_provider",
                "payment_reference",
                "payment_status",
                "payment_status_reason",
                "payment_transaction_id",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoiceurlnotobtainedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_amount",
                "payment_created_at",
                "payment_financial_institution_code",
                "payment_financial_institution_name",
                "payer_email",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "payment_provider",
                "payment_reference",
                "payment_status",
                "payment_status_reason",
                "payment_transaction_id",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        },
        "pseinvoiceurlobtainedco": {
            "direct_attributes": [
                "application_id",
                "client_id",
                "ally_slug",
                "payment_amount",
                "payment_created_at",
                "payment_financial_institution_code",
                "payment_financial_institution_name",
                "payer_email",
                "payer_id_number",
                "payer_id_type",
                "payer_type",
                "payment_provider",
                "payment_reference",
                "payment_status",
                "payment_status_reason",
                "payment_transaction_id",
                "payment_trazability_code",
                "is_marketing_opt_in",
                "ocurred_on"
            ],
            "custom_attributes": {}
        }
    },
    "unique_db_fields": {
        "direct": [
            "ally_slug",
            "application_id",
            "client_id",
            "is_marketing_opt_in",
            "ocurred_on",
            "payer_cellphone_country_code",
            "payer_cellphone_number",
            "payer_email",
            "payer_full_name",
            "payer_id_number",
            "payer_id_type",
            "payer_type",
            "payment_amount",
            "payment_created_at",
            "payment_finalized_at",
            "payment_financial_institution_code",
            "payment_financial_institution_name",
            "payment_provider",
            "payment_reference",
            "payment_rejection_code",
            "payment_rejection_description",
            "payment_status",
            "payment_status_reason",
            "payment_transaction_id",
            "payment_trazability_code"
        ],
        "custom": []
    }
}
-%}
{{ return(configuration_dict) }}
{% endmacro %}