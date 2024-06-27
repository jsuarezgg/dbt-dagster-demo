
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
allyisdisabledtooriginatebr_br AS ( 
    SELECT *
    FROM bronze.allyisdisabledtooriginatebr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,applicationcreated_br AS ( 
    SELECT *
    FROM bronze.applicationcreated_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,backgroundcheckpassedbr_br AS ( 
    SELECT *
    FROM bronze.backgroundcheckpassedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,checkoutloginsentbr_br AS ( 
    SELECT *
    FROM bronze.checkoutloginsentbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientloanapplicationcreated_br AS ( 
    SELECT *
    FROM bronze.clientloanapplicationcreated_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientpreapprovaljourneyisdisabledbr_br AS ( 
    SELECT *
    FROM bronze.clientpreapprovaljourneyisdisabledbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientwaspreapprovedbeforebr_br AS ( 
    SELECT *
    FROM bronze.clientwaspreapprovedbeforebr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,loanproposalselectedbr_br AS ( 
    SELECT *
    FROM bronze.loanproposalselectedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,pixpaymentrequestedbr_br AS ( 
    SELECT *
    FROM bronze.pixpaymentrequestedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalapplicationcompletedbr_br AS ( 
    SELECT *
    FROM bronze.preapprovalapplicationcompletedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preconditionswerevalidbr_br AS ( 
    SELECT *
    FROM bronze.preconditionswerevalidbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyaccepted_br AS ( 
    SELECT *
    FROM bronze.privacypolicyaccepted_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicyacceptedbr_br AS ( 
    SELECT *
    FROM bronze.privacypolicyacceptedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,privacypolicysent_br AS ( 
    SELECT *
    FROM bronze.privacypolicysent_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectloanapplicationcreatedfromcheckpoint_br AS ( 
    SELECT *
    FROM bronze.prospectloanapplicationcreatedfromcheckpoint_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectloanapplicationcreatedv2_br AS ( 
    SELECT *
    FROM bronze.prospectloanapplicationcreatedv2_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectupgradedtoclient_br AS ( 
    SELECT *
    FROM bronze.prospectupgradedtoclient_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwasgreaterthanmaximumconfiguredbr_br AS ( 
    SELECT *
    FROM bronze.requestedamountwasgreaterthanmaximumconfiguredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,requestedamountwaslessthanminimumconfiguredbr_br AS ( 
    SELECT *
    FROM bronze.requestedamountwaslessthanminimumconfiguredbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,backgroundcheckmaxattemptsreachedbr_br AS ( 
    SELECT *
    FROM bronze.backgroundcheckmaxattemptsreachedbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonealreadylinkedtoadifferentprospectbr_br AS ( 
    SELECT *
    FROM bronze.cellphonealreadylinkedtoadifferentprospectbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonealreadylinkedtoexistingclientbr_br AS ( 
    SELECT *
    FROM bronze.cellphonealreadylinkedtoexistingclientbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,cellphonelistedinblacklistbr_br AS ( 
    SELECT *
    FROM bronze.cellphonelistedinblacklistbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,documentlistedinblacklistbr_br AS ( 
    SELECT *
    FROM bronze.documentlistedinblacklistbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,emailalreadylinkedtoexistingclientbr_br AS ( 
    SELECT *
    FROM bronze.emailalreadylinkedtoexistingclientbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,emaillistedinblacklistbr_br AS ( 
    SELECT *
    FROM bronze.emaillistedinblacklistbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,invalidemailbr_br AS ( 
    SELECT *
    FROM bronze.invalidemailbr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,prospectalreadylinkedtodifferentcellphonebr_br AS ( 
    SELECT *
    FROM bronze.prospectalreadylinkedtodifferentcellphonebr_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,backgroundcheckpassed_br AS ( 
    SELECT *
    FROM bronze.backgroundcheckpassed_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM allyisdisabledtooriginatebr_br
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,application_date,application_email,application_id,NULL as campaign_id,channel,client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,custom_platform_version,id_number,id_type,journey_name,ocurred_on,order_id,order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM applicationcreated_br
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM backgroundcheckpassedbr_br
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM checkoutloginsentbr_br
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,application_channel_legacy,application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,custom_is_returning_client_legacy,custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientloanapplicationcreated_br
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientpreapprovaljourneyisdisabledbr_br
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientwaspreapprovedbeforebr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM loanproposalselectedbr_br
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM pixpaymentrequestedbr_br
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,preapproval_amount,preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalapplicationcompletedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preconditionswerevalidbr_br
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyaccepted_br
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,id_number,id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicyacceptedbr_br
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM privacypolicysent_br
    UNION ALL
    SELECT 
        ally_slug,application_cellphone,application_channel_legacy,application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,client_id,NULL as client_type,NULL as custom_is_bnpn_branched,custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,custom_platform_version,id_number,id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectloanapplicationcreatedfromcheckpoint_br
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,application_channel_legacy,application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,requested_amount,NULL as requested_amount_without_discount,store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectloanapplicationcreatedv2_br
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectupgradedtoclient_br
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM requestedamountwasgreaterthanmaximumconfiguredbr_br
    UNION ALL
    SELECT 
        ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,channel,NULL as client_checkout_type,client_id,client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM requestedamountwaslessthanminimumconfiguredbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM backgroundcheckmaxattemptsreachedbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM cellphonealreadylinkedtoadifferentprospectbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM cellphonealreadylinkedtoexistingclientbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM cellphonelistedinblacklistbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM documentlistedinblacklistbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM emailalreadylinkedtoexistingclientbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM emaillistedinblacklistbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM invalidemailbr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,application_cellphone,NULL as application_channel_legacy,NULL as application_date,application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,NULL as store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM prospectalreadylinkedtodifferentcellphonebr_br
    UNION ALL
    SELECT 
        NULL as ally_slug,NULL as application_cellphone,NULL as application_channel_legacy,NULL as application_date,NULL as application_email,application_id,NULL as campaign_id,NULL as channel,NULL as client_checkout_type,NULL as client_id,NULL as client_type,NULL as custom_is_bnpn_branched,NULL as custom_is_checkpoint_application_legacy,NULL as custom_is_preapproval_completed,NULL as custom_is_privacy_policy_accepted,NULL as custom_is_returning_client_legacy,NULL as custom_platform_version,NULL as id_number,NULL as id_type,NULL as journey_name,ocurred_on,NULL as order_id,NULL as order_type,NULL as preapproval_amount,NULL as preapproval_expiration_date,NULL as product,NULL as requested_amount,NULL as requested_amount_without_discount,NULL as store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM backgroundcheckpassed_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_slug,application_cellphone,application_channel_legacy,application_date,application_email,application_id,campaign_id,channel,client_checkout_type,client_id,client_type,custom_is_bnpn_branched,custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,custom_is_privacy_policy_accepted,custom_is_returning_client_legacy,custom_platform_version,id_number,id_type,journey_name,ocurred_on,order_id,order_type,preapproval_amount,preapproval_expiration_date,product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_slug,application_cellphone,application_channel_legacy,application_date,application_email,application_id,campaign_id,channel,client_checkout_type,client_id,client_type,custom_is_bnpn_branched,custom_is_checkpoint_application_legacy,custom_is_preapproval_completed,custom_is_privacy_policy_accepted,custom_is_returning_client_legacy,custom_platform_version,id_number,id_type,journey_name,last_event_ocurred_on_processed as ocurred_on,order_id,order_type,preapproval_amount,preapproval_expiration_date,product,requested_amount,requested_amount_without_discount,store_slug,store_user_id,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_pii_applications_br  
    WHERE 
    silver.f_pii_applications_br.application_id IN (SELECT DISTINCT application_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    application_id,
    element_at(array_sort(array_agg(CASE WHEN ally_slug is not null then struct(ocurred_on, ally_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_slug as ally_slug,
    element_at(array_sort(array_agg(CASE WHEN application_cellphone is not null then struct(ocurred_on, application_cellphone) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_cellphone as application_cellphone,
    element_at(array_sort(array_agg(CASE WHEN application_channel_legacy is not null then struct(ocurred_on, application_channel_legacy) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_channel_legacy as application_channel_legacy,
    element_at(array_sort(array_agg(CASE WHEN application_date is not null then struct(ocurred_on, application_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_date as application_date,
    element_at(array_sort(array_agg(CASE WHEN application_email is not null then struct(ocurred_on, application_email) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_email as application_email,
    element_at(array_sort(array_agg(CASE WHEN campaign_id is not null then struct(ocurred_on, campaign_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).campaign_id as campaign_id,
    element_at(array_sort(array_agg(CASE WHEN channel is not null then struct(ocurred_on, channel) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).channel as channel,
    element_at(array_sort(array_agg(CASE WHEN client_checkout_type is not null then struct(ocurred_on, client_checkout_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_checkout_type as client_checkout_type,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN client_type is not null then struct(ocurred_on, client_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_type as client_type,
    element_at(array_sort(array_agg(CASE WHEN custom_is_bnpn_branched is not null then struct(ocurred_on, custom_is_bnpn_branched) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_bnpn_branched as custom_is_bnpn_branched,
    element_at(array_sort(array_agg(CASE WHEN custom_is_checkpoint_application_legacy is not null then struct(ocurred_on, custom_is_checkpoint_application_legacy) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_checkpoint_application_legacy as custom_is_checkpoint_application_legacy,
    element_at(array_sort(array_agg(CASE WHEN custom_is_preapproval_completed is not null then struct(ocurred_on, custom_is_preapproval_completed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_preapproval_completed as custom_is_preapproval_completed,
    element_at(array_sort(array_agg(CASE WHEN custom_is_privacy_policy_accepted is not null then struct(ocurred_on, custom_is_privacy_policy_accepted) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_privacy_policy_accepted as custom_is_privacy_policy_accepted,
    element_at(array_sort(array_agg(CASE WHEN custom_is_returning_client_legacy is not null then struct(ocurred_on, custom_is_returning_client_legacy) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_is_returning_client_legacy as custom_is_returning_client_legacy,
    element_at(array_sort(array_agg(CASE WHEN custom_platform_version is not null then struct(ocurred_on, custom_platform_version) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).custom_platform_version as custom_platform_version,
    element_at(array_sort(array_agg(CASE WHEN id_number is not null then struct(ocurred_on, id_number) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_number as id_number,
    element_at(array_sort(array_agg(CASE WHEN id_type is not null then struct(ocurred_on, id_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).id_type as id_type,
    element_at(array_sort(array_agg(CASE WHEN journey_name is not null then struct(ocurred_on, journey_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).journey_name as journey_name,
    element_at(array_sort(array_agg(CASE WHEN order_id is not null then struct(ocurred_on, order_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).order_id as order_id,
    element_at(array_sort(array_agg(CASE WHEN order_type is not null then struct(ocurred_on, order_type) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).order_type as order_type,
    element_at(array_sort(array_agg(CASE WHEN preapproval_amount is not null then struct(ocurred_on, preapproval_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).preapproval_amount as preapproval_amount,
    element_at(array_sort(array_agg(CASE WHEN preapproval_expiration_date is not null then struct(ocurred_on, preapproval_expiration_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).preapproval_expiration_date as preapproval_expiration_date,
    element_at(array_sort(array_agg(CASE WHEN product is not null then struct(ocurred_on, product) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).product as product,
    element_at(array_sort(array_agg(CASE WHEN requested_amount is not null then struct(ocurred_on, requested_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).requested_amount as requested_amount,
    element_at(array_sort(array_agg(CASE WHEN requested_amount_without_discount is not null then struct(ocurred_on, requested_amount_without_discount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).requested_amount_without_discount as requested_amount_without_discount,
    element_at(array_sort(array_agg(CASE WHEN store_slug is not null then struct(ocurred_on, store_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_slug as store_slug,
    element_at(array_sort(array_agg(CASE WHEN store_user_id is not null then struct(ocurred_on, store_user_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).store_user_id as store_user_id,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    application_id
                       
           )


, final AS (
    SELECT 
        *,
        date(last_event_ocurred_on_processed ) as ocurred_on_date,
        to_timestamp('2022-01-01') updated_at
    FROM grouped_events 
)

select * from final;

/* DEBUGGING SECTION
is_incremental: True
this: silver.f_pii_applications_br
country: br
silver_table_name: f_pii_applications_br
table_pk_fields: ['application_id']
table_pk_amount: 1
fields_direct: ['ally_slug', 'application_cellphone', 'application_channel_legacy', 'application_date', 'application_email', 'application_id', 'campaign_id', 'channel', 'client_checkout_type', 'client_id', 'client_type', 'custom_is_bnpn_branched', 'custom_is_checkpoint_application_legacy', 'custom_is_preapproval_completed', 'custom_is_privacy_policy_accepted', 'custom_is_returning_client_legacy', 'custom_platform_version', 'id_number', 'id_type', 'journey_name', 'ocurred_on', 'order_id', 'order_type', 'preapproval_amount', 'preapproval_expiration_date', 'product', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'allyisdisabledtooriginatebr': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'applicationcreated': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_date', 'application_email', 'channel', 'client_checkout_type', 'client_id', 'client_type', 'custom_platform_version', 'id_number', 'id_type', 'journey_name', 'order_id', 'order_type', 'product', 'requested_amount', 'requested_amount_without_discount', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'backgroundcheckpassedbr': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'checkoutloginsentbr': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'clientloanapplicationcreated': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_date', 'application_email', 'application_channel_legacy', 'client_id', 'custom_is_returning_client_legacy', 'custom_platform_version', 'id_number', 'id_type', 'requested_amount', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'clientpreapprovaljourneyisdisabledbr': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'clientwaspreapprovedbeforebr': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'loanproposalselectedbr': {'direct_attributes': ['application_id', 'campaign_id', 'ocurred_on'], 'custom_attributes': {}}, 'pixpaymentrequestedbr': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'custom_is_bnpn_branched', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'preapprovalapplicationcompletedbr': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'custom_is_preapproval_completed', 'journey_name', 'preapproval_amount', 'preapproval_expiration_date', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'preconditionswerevalidbr': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicyaccepted': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'application_channel_legacy', 'client_id', 'custom_is_privacy_policy_accepted', 'id_number', 'id_type', 'requested_amount', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicyacceptedbr': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_email', 'channel', 'campaign_id', 'client_id', 'client_type', 'custom_is_privacy_policy_accepted', 'journey_name', 'id_number', 'id_type', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'privacypolicysent': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'client_id', 'custom_platform_version', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'prospectloanapplicationcreatedfromcheckpoint': {'direct_attributes': ['application_id', 'ally_slug', 'application_cellphone', 'application_date', 'application_email', 'application_channel_legacy', 'client_id', 'custom_is_checkpoint_application_legacy', 'custom_platform_version', 'id_number', 'id_type', 'requested_amount', 'store_slug', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'prospectloanapplicationcreatedv2': {'direct_attributes': ['application_id', 'ally_slug', 'application_date', 'application_channel_legacy', 'client_id', 'custom_platform_version', 'requested_amount', 'store_slug', 'store_user_id', 'order_id', 'ocurred_on'], 'custom_attributes': {}}, 'prospectupgradedtoclient': {'direct_attributes': ['application_id', 'application_email', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwasgreaterthanmaximumconfiguredbr': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'requestedamountwaslessthanminimumconfiguredbr': {'direct_attributes': ['application_id', 'ally_slug', 'channel', 'client_id', 'client_type', 'journey_name', 'product', 'ocurred_on'], 'custom_attributes': {}}, 'backgroundcheckmaxattemptsreachedbr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'cellphonealreadylinkedtoadifferentprospectbr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'cellphonealreadylinkedtoexistingclientbr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'cellphonelistedinblacklistbr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'documentlistedinblacklistbr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'emailalreadylinkedtoexistingclientbr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'emaillistedinblacklistbr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'invalidemailbr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'prospectalreadylinkedtodifferentcellphonebr': {'direct_attributes': ['application_id', 'application_cellphone', 'application_email', 'ocurred_on'], 'custom_attributes': {}}, 'backgroundcheckpassed': {'direct_attributes': ['application_id', 'store_user_id', 'ocurred_on'], 'custom_attributes': {}}}
events_keys: ['allyisdisabledtooriginatebr', 'applicationcreated', 'backgroundcheckpassedbr', 'checkoutloginsentbr', 'clientloanapplicationcreated', 'clientpreapprovaljourneyisdisabledbr', 'clientwaspreapprovedbeforebr', 'loanproposalselectedbr', 'pixpaymentrequestedbr', 'preapprovalapplicationcompletedbr', 'preconditionswerevalidbr', 'privacypolicyaccepted', 'privacypolicyacceptedbr', 'privacypolicysent', 'prospectloanapplicationcreatedfromcheckpoint', 'prospectloanapplicationcreatedv2', 'prospectupgradedtoclient', 'requestedamountwasgreaterthanmaximumconfiguredbr', 'requestedamountwaslessthanminimumconfiguredbr', 'backgroundcheckmaxattemptsreachedbr', 'cellphonealreadylinkedtoadifferentprospectbr', 'cellphonealreadylinkedtoexistingclientbr', 'cellphonelistedinblacklistbr', 'documentlistedinblacklistbr', 'emailalreadylinkedtoexistingclientbr', 'emaillistedinblacklistbr', 'invalidemailbr', 'prospectalreadylinkedtodifferentcellphonebr', 'backgroundcheckpassed']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
