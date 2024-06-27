
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ab_testing_started
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ab_testing_variant_started
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_accept_counter_offer_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_additional_information_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_additional_information_br_form_submitted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_additional_information_santander_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_additional_information_santander_co_form_submitted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_additional_information_v2_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_deal_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_deal_tapped_from_discounts
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_click_end_purchase
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_display_payment_method
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_displayed_payment_method
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_fully_page_loaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_get_ally_config
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_init_payment_modal
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_payment_method
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_redirected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_integration_checkout_selected_payment_method
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ally_management_accept_privacy_policy
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_amout_to_pay_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ap_paylink_cell_phone_number_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ap_paylink_email_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ap_paylink_id_number_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ap_paylink_last_name_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ap_paylink_order_description_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_ap_paylink_request_amount_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_cancellation_cancel_tx_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_cancellation_continue_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_cancellation_filter_applied
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_cancellation_tx_confirmation_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_change_table_page
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_close_campaign
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_create_application
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_csv_downloaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_download_csv
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_forbidden
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_init
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_init_paylink
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_logout
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_menu_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_open_campaign
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_paylink_created
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_paylink_failure
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_paylink_reset
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_paylink_submited
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_payment_claim_button_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_payment_file_downloaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_payment_filter_applied
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_payment_many_files_download
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_payment_row_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_payment_tab_type_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_redirect_campaign
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_redirected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_repeated_ally
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_set_user
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_store_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_support_click
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_table_page_changed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_tx_change_table_page_changed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_tx_filter_applied
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_tx_status_detail_copied
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_tx_status_detail_copy
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_tx_status_detail_print_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_tx_status_detail_refund_detail_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_unauthorized
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_welcome
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_ap_welcome_showed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_application_installed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_application_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_application_updated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_convert_preapproval
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_cupo_modal_info_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_get_logrocket_session
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_host_origin_restricted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_in_store_application_created
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_in_store_application_failure
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_in_store_id_number_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_in_store_request_amount_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_in_store_submitted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_init_checkout
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_init_paylink
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_init_pos
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_last_reload
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_link_triggered
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_logout
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_navbar_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_notifications_modal_later_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_notifications_modal_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_notifications_modal_turned_on_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_paylink_created
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_paylink_failure
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_paylink_reset
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_paylink_submited
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_screen_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_service_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_set_action_loading
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_set_agent_status
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_set_ally
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_set_app
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_set_loading
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_set_tracker
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_set_user
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_set_version_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_store_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_app_update_agent_status
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_application_backgrounded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_application_change_empty_event
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_application_installed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_application_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_application_updated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_auth_get_pre_approved_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_auth_login_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_auth_login_resended_otp
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_auth_login_success
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_auth_pre_approved_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_auth_pre_approved_login_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_auth_pre_approved_success
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_auth_registration_send_code_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_background_check_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_background_check_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_banking_license_partner_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_banking_license_partner_br_select_download_receipt
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_banking_license_partner_br_survey_view
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_basic_identity_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_basic_identity_br_recover_application_from_bio_info_did_not_match
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_basic_identity_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_basic_identity_co_recover_application_from_bio_info_did_not_match
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_bn_pn_payments_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_bn_pn_payments_br_bn_pn_payments_br_pix_payment
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cancel_auxiliary_polling
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_categories_select_return
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_categories_select_search_bar
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_categories_select_store
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_categories_view_stores
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_category_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cellphone_validation_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cellphone_validation_br_send_cellphone_validation_notifications
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cellphone_validation_br_validated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cellphone_validation_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cellphone_validation_send_notifications
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cellphone_validation_validate
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_communications_cupo_failing
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_corresponsal_download_receipt_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_counter_offer_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_frozen_message
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_ultra_frozen_message
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_go_to_account
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_go_to_account_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_log_out_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_login_client_portal
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_login_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_login_success
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_login_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_phone_not_recognized
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_request_otp
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_screen_login_rendered
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_screen_no_cupo_info_rendered
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_screen_otp_rendered
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_ally_login
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_close
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_close_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_edit_cellphone_number
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_info_return
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_login
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_login_mail
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_login_return
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_pay
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_select_view_cupo
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_send_again_otp
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_send_otp_again
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_talk_with_support_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_tapped_from_in_app_browser
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_try_login_again
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_view_cupo
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_view_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_view_login
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_view_login_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_where_to_buy
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_cupo_widget_where_to_buy_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_deals_category_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_decline_counter_offer_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_delete_favourites
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_device_information
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_device_information_info_updated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_dia_dela_amistad_first_segment
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_dia_dela_amistad_forth_segment
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_dia_dela_amistad_second_segment
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_dia_dela_amistad_third_segment
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_discounts_ally_deal_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_discounts_deals_category_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_discounts_home_deals_see_all_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_down_payment_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_down_payment_br_experiment_started
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_down_payment_br_retry_payment_pix
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_down_payment_br_select_copy_pix
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_embedded_track_onboarding_page
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_expedited_checkout_login_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_expedited_checkout_login_co_accepted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_expedited_checkout_login_co_restarted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_expedited_checkout_transaction_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_expedited_checkout_transaction_co_completed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_expedited_loan_acceptance_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_expedited_loan_acceptance_co_validated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_expedited_loan_proposals_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_feature_section_seen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_featured_section_ally_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_featured_section_see_all_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_featured_section_seen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_first_cupo_increased_message
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_flow_into_iframe
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_fraud_check_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_fraud_check_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_fraud_check_rc_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_fraud_check_rc_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_get_favourites
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_get_pre_approved_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_client_arrived
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_communications_cupo_failing
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_cupo_frozen_message
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_cupo_ultra_frozen_message
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_deals_see_all_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_delete_favorite_store
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_featured_section_ally_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_featured_section_see_all_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_featured_section_seen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_first_cupo_increased_message
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_new_available_cupo_increased_message
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_promoted_banner_seen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_promoted_banner_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_recently_visited_ally_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_recently_visited_section_seen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_screen_ecm_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_screen_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_see_all_categories_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_see_all_stores_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_select_cupo_check
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_select_cupo_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_select_cupo_modal_info_close
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_select_favorite_store
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_settings_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_home_store_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_back_document_photo_uploaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_black_photos_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_camera_permission_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_centralize_face
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_collected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_confirmation_screen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_copy_url_to_clipboard
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_face_centralized
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_front_document_photo_uploaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_no_face_detected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_selfie_feedback
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_selfie_photo_uploaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_started
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_unsupported_browser
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_photos_unsupported_desktop
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_wa
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_identity_wa_recover_called
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_admin_cancel_applications_polling
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_admin_cancel_metrics_polling
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_admin_metrics_loaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_admin_reload_applications
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_admin_reload_metrics
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_admin_scheduler_applications_loaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_admin_start_applications_polling
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_admin_start_metrics_polling
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_add_face_to_trueface
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_assignation_loaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_cancel_heartbeat
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_cancel_pending_applications_polling
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_cancel_polling_assignation
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_clean_current_assignation
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_keep_request_photo_updated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_keep_request_update
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_remove_face_from_trueface
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_request_photo_updated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_request_reload_active
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_request_reload_photos_active
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_request_update
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_requests_loaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_start_heartbeat
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_start_pending_applications_polling
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_start_polling_assignation
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_support_agent_loaded
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_support_agent_photo_updated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_dashboard_support_agent_updated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_third_party
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_third_party_camera_permission_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_third_party_copy_url_to_clipboard
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_third_party_sdk_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_third_party_started
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_idv_third_party_unsupported_browser
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_legal_ap_error_step
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_legal_ap_get_legal_journey
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_legal_ap_load_step_information
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_legal_ap_next_step
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_legal_complete_step
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_co_loan_accepted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_co_select_download_app
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_co_select_return_store
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_co_send_notifications
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_co_validated
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_santander_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_santander_co_loan_accepted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_santander_co_recover_application_from_bio_info_did_not_match
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_acceptance_v2_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposal_sem_juros_experiment
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_br_loan_proposal_sem_juros_experiment
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_br_loan_proposals_step_viewed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_br_pago_flex
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_br_proposal_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_br_proposal_submitted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_br_proposal_summary_step_viewed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_change_payment_date_back_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_change_payment_date_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_confirmed_payment_date
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_flex_default_proposal_experiment_started
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_loan_proposals_step_viewed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_payment_date_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_proposal_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_proposal_submitted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_co_proposal_summary_step_viewed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_santander_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_santander_co_proposal_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loan_proposals_santander_co_proposal_submitted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_loanacceptedco
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_login_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_login_resended_otp
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_login_success
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_marketing_shopping_intent
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_navbar_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_new_available_cupo_increased_message
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_notifications_pop_up_dismissed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_notifications_pop_up_taped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_notifications_push_notification_taped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_open_in_new_tab
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_opened_web_page
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_appliaction_polling_cancellation_requested
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_appliaction_polling_requested
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_application_declined
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_application_exited
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_application_recovered
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_application_restarted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_application_retried
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_application_unauthorized
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_email_is_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_email_is_rejected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_email_is_valid
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_issue_date_filled_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_mobile_phone_edited
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_originations_stage_request_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pago_br_show_pago_stepper_experiment_started
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pay_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_paylink_cell_phone_number_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_paylink_email_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_paylink_id_number_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_paylink_last_name_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_paylink_request_amount_filled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_code_copy_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_continue_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_corresponsal_continue_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_history_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_info_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_method_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_pix_copy_code_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_pix_pay_different_value_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_pse_continue_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_pse_continue_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_tab_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_transaction_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payment_transaction_success
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_paymentpix_status_inactive
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_paymentpix_status_paid_out
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_br_pix_copy_code_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_br_pix_form_paynow_button_touched
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_br_pix_pay_different_value_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_br_status_inactive
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_br_status_paid_out
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_br_transaction_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_br_transaction_success
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_amout_to_pay_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_continue_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_corresponsal_continue_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_corresponsal_download_receipt_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_corresponsal_home_screen_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_method_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_pse_continue_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_pse_continue_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_pse_disabled
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_saved_payment_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_transaction_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_transaction_success
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_use_another_method_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_co_wompi_response_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_history_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_info_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_pay_button_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_purchase_history_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_tab_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_back_button
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_corresponsal_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_home_add_account_state
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_home_add_account_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_home_continue_tap
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_home_know_debt_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_loggin_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_loggin_screen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_loggin_screen_continue_button
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_loggin_screen_otp_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_loggin_screen_pay_without_phone
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_loggin_screen_request_otp
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_loggin_success
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_pix_copy
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_pix_error_creation
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_pix_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_pix_screen_exit
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_pse_states
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_pse_webview
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_payments_web_select_payment_method
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pdp_script_script_started
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pdp_script_widget_clicked
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pdp_script_widget_inserted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pdp_script_widget_not_displayed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pdp_script_widget_not_inserted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pdp_script_widget_rendered
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_personal_information_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_personal_information_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pix_form_paynow_button_touched
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pix_instructions_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pop_up_dismissed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pop_up_taped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_pre_approved_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_preapproval_summary_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_preapproval_summary_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_preconditions_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_preconditions_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_preconditions_pago_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_br_policy_accepted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_co_policy_accepted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_santander_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_santander_co_policy_accepted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_v2_accepted_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_v2_accepted_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_v2_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_privacy_policy_v2_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_promoted_banner_seen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_promoted_banner_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_psychometric_assessment
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_purchase_history_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_push_notification_taped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_recently_visited_ally_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_recently_visited_section_seen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_registration_send_code_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_risk_evaluation_santander_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_sales_generation_service_screen_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_sales_generation_service_tab_seen
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_saved_payment_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_screen_opened
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_search_bar_not_found
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_search_bar_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_select_confirm_sales_generation_service
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_select_finish_purchase
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_select_sales_generation_service_toggle
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_select_sort_allies
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_service_error
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_set_banners
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_set_categories
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_set_favourites
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_set_notification
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_set_user_data
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_set_version_info
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_settings_notification_modal_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_settings_notification_toggle_switched
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_category_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_search_bar_not_found
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_search_bar_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_select_remove_search_result
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_select_return
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_select_sort_allies
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_select_sort_category
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_store_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_shop_subcategory_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_start_auxiliary_polling
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_store_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_store_tapped_from_shop
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_stores_select_category
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_stores_select_cupo_check
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_stores_select_search_bar
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_stores_select_store
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_stores_view_search_results
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_stores_view_stores
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_subcategory_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_subproduct_selection_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_subproduct_selection_br_product_selected
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_terms_ap_terms_accepted
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_transaction_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_underwriting_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_underwriting_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_underwriting_pago_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_underwriting_preapproval_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_underwriting_preapproval_pago_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_underwriting_rc_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_underwriting_rc_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_underwriting_rc_pago_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_update_additional_information_v2_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_update_latest_polling_assignation
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_update_personal_information_br
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_update_personal_information_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_use_another_method_tapped
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_user_logout
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_wompi_response_failed
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_work_information_santander_co
    GROUP BY 2
    
    UNION ALL
    
    SELECT
        MAX(lower(REPLACE(event_type, ".", "_"))) AS event_type,
        event_time::date AS event_date,
        COUNT(1) AS count_of_events,
        MAX(NOW()) AS ingested_at,
        MAX(to_timestamp('2022-01-01')) AS updated_at
    FROM bronze.amplitude_addi_funnel_work_information_santander_co_form_submitted
    GROUP BY 2
    
    