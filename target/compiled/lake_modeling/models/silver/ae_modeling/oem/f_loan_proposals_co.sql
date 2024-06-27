
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
conditionalcreditcheckpassed_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassed_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassed_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassed_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassed_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassed_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedwithinsufficientaddicupo_unnested_by_loan_proposal_event_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedwithinsufficientaddicupo_unnested_by_loan_proposal_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,allyapplicationupdated_co AS ( 
    SELECT *
    FROM bronze.allyapplicationupdated_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckpassedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckpassedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalconditionalcreditcheckpassedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.preapprovalconditionalcreditcheckpassedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckapprovedsantanderco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.creditcheckapprovedsantanderco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.creditcheckfailedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpsychometricpassedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.creditcheckpsychometricpassedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,firstpaymentdatechangedco_co AS ( 
    SELECT *
    FROM bronze.firstpaymentdatechangedco_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpsychometricpassedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpsychometricpassedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpsychometricfailedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.creditcheckpsychometricfailedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckfailedco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckfailedco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedpagoco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedpagoco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedpagoco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.creditcheckfailedpagoco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedpagoco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.creditcheckpassedpagoco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredpagoco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredpagoco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedpagoco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedpagoco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedpagoco_unnested_by_loan_proposals_event_co AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedpagoco_unnested_by_loan_proposals_event_co
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassed_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassed_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassed_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,NULL as first_loan_cash,first_loan_npv,NULL as first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,NULL as lifetime_cash,lifetime_npv,NULL as lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassedwithinsufficientaddicupo_unnested_by_loan_proposal_event_co
    UNION ALL
    SELECT 
        NULL as ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,NULL as approved_amount,client_id,NULL as contribution_margin,NULL as decision_npv,NULL as discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,NULL as first_loan_cash,NULL as first_loan_npv,NULL as first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,NULL as interest_rate,NULL as lbl,NULL as learning_population,NULL as lifetime_cash,NULL as lifetime_npv,NULL as lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,NULL as returning_client,NULL as term,NULL as total_fga_rate,NULL as total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM allyapplicationupdated_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM psychometricevaluationisrequiredco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,NULL as decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,NULL as first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,NULL as lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalcreditcheckpassedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,NULL as decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,NULL as first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,NULL as lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalconditionalcreditcheckpassedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,NULL as decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,NULL as first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,NULL as lifetime_npv,lifetime_roe,loan_proposal_id,loan_subvention_applied,loan_subvention_iva,loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckapprovedsantanderco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckfailedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpsychometricpassedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        NULL as ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,NULL as ally_slug,NULL as application_id,NULL as approved_amount,NULL as client_id,NULL as contribution_margin,NULL as decision_npv,NULL as discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,NULL as first_loan_cash,NULL as first_loan_npv,NULL as first_loan_roe,first_payment_date,NULL as ia_pd_multiplier,NULL as interest_rate,NULL as lbl,NULL as learning_population,NULL as lifetime_cash,NULL as lifetime_npv,NULL as lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,NULL as term,NULL as total_fga_rate,NULL as total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM firstpaymentdatechangedco_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpsychometricpassedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpsychometricfailedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckfailedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,NULL as decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,NULL as first_loan_npv,first_loan_roe,NULL as first_payment_date,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,NULL as lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalcreditcheckfailedco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassedpagoco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckfailedpagoco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassedpagoco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM psychometricevaluationisrequiredpagoco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckfailedpagoco_unnested_by_loan_proposals_event_co
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,NULL as fga_client_rate,NULL as fga_comission_rate,NULL as fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,NULL as loan_subvention_applied,NULL as loan_subvention_iva,NULL as loan_subvention_value,ocurred_on,returning_client,term,NULL as total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassedpagoco_unnested_by_loan_proposals_event_co
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,loan_subvention_applied,loan_subvention_iva,loan_subvention_value,ocurred_on,returning_client,term,total_fga_rate,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,fga_client_rate,fga_comission_rate,fga_tax_rate,first_loan_cash,first_loan_npv,first_loan_roe,first_payment_date,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,loan_subvention_applied,loan_subvention_iva,loan_subvention_value,last_event_ocurred_on_processed as ocurred_on,returning_client,term,total_fga_rate,total_interest,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_loan_proposals_co  
    WHERE 
    silver.f_loan_proposals_co.loan_proposal_id IN (SELECT DISTINCT loan_proposal_id FROM union_bronze)      
    
)   

-- SECTION 3 -> merge events by key and keep last not null data in each field
  , grouped_events AS (
  select
    loan_proposal_id,
    element_at(array_sort(array_agg(CASE WHEN ally_mdf is not null then struct(ocurred_on, ally_mdf) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_mdf as ally_mdf,
    element_at(array_sort(array_agg(CASE WHEN ally_mdf_cancellation is not null then struct(ocurred_on, ally_mdf_cancellation) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_mdf_cancellation as ally_mdf_cancellation,
    element_at(array_sort(array_agg(CASE WHEN ally_mdf_fraud is not null then struct(ocurred_on, ally_mdf_fraud) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_mdf_fraud as ally_mdf_fraud,
    element_at(array_sort(array_agg(CASE WHEN ally_slug is not null then struct(ocurred_on, ally_slug) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ally_slug as ally_slug,
    element_at(array_sort(array_agg(CASE WHEN application_id is not null then struct(ocurred_on, application_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).application_id as application_id,
    element_at(array_sort(array_agg(CASE WHEN approved_amount is not null then struct(ocurred_on, approved_amount) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).approved_amount as approved_amount,
    element_at(array_sort(array_agg(CASE WHEN client_id is not null then struct(ocurred_on, client_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).client_id as client_id,
    element_at(array_sort(array_agg(CASE WHEN contribution_margin is not null then struct(ocurred_on, contribution_margin) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).contribution_margin as contribution_margin,
    element_at(array_sort(array_agg(CASE WHEN decision_npv is not null then struct(ocurred_on, decision_npv) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).decision_npv as decision_npv,
    element_at(array_sort(array_agg(CASE WHEN discount_rate is not null then struct(ocurred_on, discount_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).discount_rate as discount_rate,
    element_at(array_sort(array_agg(CASE WHEN fga_client_rate is not null then struct(ocurred_on, fga_client_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).fga_client_rate as fga_client_rate,
    element_at(array_sort(array_agg(CASE WHEN fga_comission_rate is not null then struct(ocurred_on, fga_comission_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).fga_comission_rate as fga_comission_rate,
    element_at(array_sort(array_agg(CASE WHEN fga_tax_rate is not null then struct(ocurred_on, fga_tax_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).fga_tax_rate as fga_tax_rate,
    element_at(array_sort(array_agg(CASE WHEN first_loan_cash is not null then struct(ocurred_on, first_loan_cash) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_loan_cash as first_loan_cash,
    element_at(array_sort(array_agg(CASE WHEN first_loan_npv is not null then struct(ocurred_on, first_loan_npv) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_loan_npv as first_loan_npv,
    element_at(array_sort(array_agg(CASE WHEN first_loan_roe is not null then struct(ocurred_on, first_loan_roe) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_loan_roe as first_loan_roe,
    element_at(array_sort(array_agg(CASE WHEN first_payment_date is not null then struct(ocurred_on, first_payment_date) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_payment_date as first_payment_date,
    element_at(array_sort(array_agg(CASE WHEN ia_pd_multiplier is not null then struct(ocurred_on, ia_pd_multiplier) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ia_pd_multiplier as ia_pd_multiplier,
    element_at(array_sort(array_agg(CASE WHEN interest_rate is not null then struct(ocurred_on, interest_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).interest_rate as interest_rate,
    element_at(array_sort(array_agg(CASE WHEN lbl is not null then struct(ocurred_on, lbl) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lbl as lbl,
    element_at(array_sort(array_agg(CASE WHEN learning_population is not null then struct(ocurred_on, learning_population) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).learning_population as learning_population,
    element_at(array_sort(array_agg(CASE WHEN lifetime_cash is not null then struct(ocurred_on, lifetime_cash) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lifetime_cash as lifetime_cash,
    element_at(array_sort(array_agg(CASE WHEN lifetime_npv is not null then struct(ocurred_on, lifetime_npv) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lifetime_npv as lifetime_npv,
    element_at(array_sort(array_agg(CASE WHEN lifetime_roe is not null then struct(ocurred_on, lifetime_roe) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lifetime_roe as lifetime_roe,
    element_at(array_sort(array_agg(CASE WHEN loan_subvention_applied is not null then struct(ocurred_on, loan_subvention_applied) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_subvention_applied as loan_subvention_applied,
    element_at(array_sort(array_agg(CASE WHEN loan_subvention_iva is not null then struct(ocurred_on, loan_subvention_iva) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_subvention_iva as loan_subvention_iva,
    element_at(array_sort(array_agg(CASE WHEN loan_subvention_value is not null then struct(ocurred_on, loan_subvention_value) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).loan_subvention_value as loan_subvention_value,
    element_at(array_sort(array_agg(CASE WHEN returning_client is not null then struct(ocurred_on, returning_client) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).returning_client as returning_client,
    element_at(array_sort(array_agg(CASE WHEN term is not null then struct(ocurred_on, term) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).term as term,
    element_at(array_sort(array_agg(CASE WHEN total_fga_rate is not null then struct(ocurred_on, total_fga_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).total_fga_rate as total_fga_rate,
    element_at(array_sort(array_agg(CASE WHEN total_interest is not null then struct(ocurred_on, total_interest) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).total_interest as total_interest,
    element_at(array_sort(array_agg(CASE WHEN last_event_name_processed is not null then struct(ocurred_on, last_event_name_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_name_processed as last_event_name_processed,
    element_at(array_sort(array_agg(CASE WHEN event_name is not null then struct(ocurred_on, event_name) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_name as event_name,
    element_at(array_sort(array_agg(CASE WHEN last_event_id_processed is not null then struct(ocurred_on, last_event_id_processed) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).last_event_id_processed as last_event_id_processed,
    element_at(array_sort(array_agg(CASE WHEN event_id is not null then struct(ocurred_on, event_id) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).event_id as event_id,
    max(ocurred_on) as last_event_ocurred_on_processed
  from union_all_events
  group by 
                    loan_proposal_id
                       
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
this: silver.f_loan_proposals_co
country: co
silver_table_name: f_loan_proposals_co
table_pk_fields: ['loan_proposal_id']
table_pk_amount: 1
fields_direct: ['ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'application_id', 'approved_amount', 'client_id', 'contribution_margin', 'decision_npv', 'discount_rate', 'fga_client_rate', 'fga_comission_rate', 'fga_tax_rate', 'first_loan_cash', 'first_loan_npv', 'first_loan_roe', 'first_payment_date', 'ia_pd_multiplier', 'interest_rate', 'lbl', 'learning_population', 'lifetime_cash', 'lifetime_npv', 'lifetime_roe', 'loan_proposal_id', 'loan_subvention_applied', 'loan_subvention_iva', 'loan_subvention_value', 'ocurred_on', 'returning_client', 'term', 'total_fga_rate', 'total_interest']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'conditionalcreditcheckpassed_unnested_by_loan_proposals_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'creditcheckpassed_unnested_by_loan_proposals_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckpassed_unnested_by_loan_proposals_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckpassedwithinsufficientaddicupo_unnested_by_loan_proposal_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'AllyApplicationUpdated': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'application_id', 'client_id', 'ally_slug', 'ocurred_on'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'creditcheckpassedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckpassedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_rc_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'psychometricevaluationisrequiredco_unnested_by_loan_proposals_event': {'stage': 'underwriting_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'preapprovalcreditcheckpassedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_preapproval_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'preapprovalconditionalcreditcheckpassedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_preapproval_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'creditcheckapprovedsantanderco_unnested_by_loan_proposals_event': {'stage': 'risk_evaluation_santander_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'loan_subvention_applied', 'loan_subvention_value', 'loan_subvention_iva', 'total_interest'], 'custom_attributes': {}}, 'creditcheckfailedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'creditcheckpsychometricpassedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_psychometric_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'FirstPaymentDateChangedCO': {'stage': 'loan_proposals_co', 'direct_attributes': ['loan_proposal_id', 'ocurred_on', 'first_payment_date', 'returning_client'], 'custom_attributes': {}}, 'conditionalcreditcheckpsychometricpassedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_psychometric_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'creditcheckpsychometricfailedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_psychometric_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckfailedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_rc_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'preapprovalcreditcheckfailedco_unnested_by_loan_proposals_event': {'stage': 'underwriting_preapproval_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedpagoco_unnested_by_loan_proposals_event': {'stage': 'underwriting_pago_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'creditcheckfailedpagoco_unnested_by_loan_proposals_event': {'stage': 'underwriting_pago_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'creditcheckpassedpagoco_unnested_by_loan_proposals_event': {'stage': 'underwriting_pago_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'psychometricevaluationisrequiredpagoco_unnested_by_loan_proposals_event': {'stage': 'underwriting_pago_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'total_fga_rate', 'fga_tax_rate', 'fga_comission_rate', 'fga_client_rate', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckfailedpagoco_unnested_by_loan_proposals_event': {'stage': 'underwriting_rc_pago_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckpassedpagoco_unnested_by_loan_proposals_event': {'stage': 'underwriting_rc_pago_co', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}}
events_keys: ['conditionalcreditcheckpassed_unnested_by_loan_proposals_event', 'creditcheckpassed_unnested_by_loan_proposals_event', 'clientcreditcheckpassed_unnested_by_loan_proposals_event', 'conditionalcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event', 'clientcreditcheckpassedwithlowerapprovedamount_unnested_by_loan_proposals_event', 'clientcreditcheckpassedwithinsufficientaddicupo_unnested_by_loan_proposal_event', 'AllyApplicationUpdated', 'conditionalcreditcheckpassedco_unnested_by_loan_proposals_event', 'creditcheckpassedco_unnested_by_loan_proposals_event', 'clientcreditcheckpassedco_unnested_by_loan_proposals_event', 'psychometricevaluationisrequiredco_unnested_by_loan_proposals_event', 'preapprovalcreditcheckpassedco_unnested_by_loan_proposals_event', 'preapprovalconditionalcreditcheckpassedco_unnested_by_loan_proposals_event', 'creditcheckapprovedsantanderco_unnested_by_loan_proposals_event', 'creditcheckfailedco_unnested_by_loan_proposals_event', 'creditcheckpsychometricpassedco_unnested_by_loan_proposals_event', 'FirstPaymentDateChangedCO', 'conditionalcreditcheckpsychometricpassedco_unnested_by_loan_proposals_event', 'creditcheckpsychometricfailedco_unnested_by_loan_proposals_event', 'clientcreditcheckfailedco_unnested_by_loan_proposals_event', 'preapprovalcreditcheckfailedco_unnested_by_loan_proposals_event', 'conditionalcreditcheckpassedpagoco_unnested_by_loan_proposals_event', 'creditcheckfailedpagoco_unnested_by_loan_proposals_event', 'creditcheckpassedpagoco_unnested_by_loan_proposals_event', 'psychometricevaluationisrequiredpagoco_unnested_by_loan_proposals_event', 'clientcreditcheckfailedpagoco_unnested_by_loan_proposals_event', 'clientcreditcheckpassedpagoco_unnested_by_loan_proposals_event']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
