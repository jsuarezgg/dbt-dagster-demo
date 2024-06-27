
-- SILVER SQL

-- SECTION 1 -> CALLING BRONCE ref's
WITH
conditionalcreditcheckpassed_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassed_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassed_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassed_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassed_unnested_by_loan_proposal_event_br AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassed_unnested_by_loan_proposal_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,conditionalcreditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.conditionalcreditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassedbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.creditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckpassedbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.clientcreditcheckpassedbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,psychometricevaluationisrequiredbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.psychometricevaluationisrequiredbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,creditcheckfailedbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.creditcheckfailedbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,clientcreditcheckfailedbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.clientcreditcheckfailedbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalconditionalcreditcheckpassedbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.preapprovalconditionalcreditcheckpassedbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckfailedbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckfailedbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)
,preapprovalcreditcheckpassedbr_unnested_by_loan_proposals_event_br AS ( 
    SELECT *
    FROM bronze.preapprovalcreditcheckpassedbr_unnested_by_loan_proposals_event_br
    WHERE ocurred_on_date BETWEEN (to_date('2022-01-01'- INTERVAL "10" HOUR)) AND to_date('2022-01-30') AND
        ocurred_on BETWEEN (to_timestamp('2022-01-01'- INTERVAL "10" HOUR)) AND to_timestamp('2022-01-30') 
)


--SECTION 1B -> UNION BRONCE ref's
, union_bronze AS (
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassed_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassed_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassed_unnested_by_loan_proposal_event_br
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,NULL as decision_npv,discount_rate,first_loan_cash,NULL as first_loan_npv,first_loan_roe,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,NULL as lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassedbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM conditionalcreditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassedbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckpassedbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM psychometricevaluationisrequiredbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM creditcheckfailedbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM clientcreditcheckfailedbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,NULL as decision_npv,discount_rate,first_loan_cash,NULL as first_loan_npv,first_loan_roe,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,NULL as lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalconditionalcreditcheckpassedbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,NULL as decision_npv,discount_rate,first_loan_cash,NULL as first_loan_npv,first_loan_roe,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,NULL as lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalcreditcheckfailedbr_unnested_by_loan_proposals_event_br
    UNION ALL
    SELECT 
        ally_mdf,NULL as ally_mdf_cancellation,NULL as ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,NULL as decision_npv,discount_rate,first_loan_cash,NULL as first_loan_npv,first_loan_roe,NULL as ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,NULL as lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM preapprovalcreditcheckpassedbr_unnested_by_loan_proposals_event_br
    
) 
-- SECTION 2 -> UNION of prepared CTE's in Section 1
, union_all_events AS (
    SELECT 
    ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,ocurred_on,returning_client,term,total_interest,event_name as last_event_name_processed,
    event_name,event_id as last_event_id_processed,
    event_id
    FROM union_bronze 
    UNION ALL
    SELECT 
    ally_mdf,ally_mdf_cancellation,ally_mdf_fraud,ally_slug,application_id,approved_amount,client_id,contribution_margin,decision_npv,discount_rate,first_loan_cash,first_loan_npv,first_loan_roe,ia_pd_multiplier,interest_rate,lbl,learning_population,lifetime_cash,lifetime_npv,lifetime_roe,loan_proposal_id,last_event_ocurred_on_processed as ocurred_on,returning_client,term,total_interest,last_event_name_processed,
    event_name,last_event_id_processed,
    event_id
    FROM silver.f_loan_proposals_br  
    WHERE 
    silver.f_loan_proposals_br.loan_proposal_id IN (SELECT DISTINCT loan_proposal_id FROM union_bronze)      
    
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
    element_at(array_sort(array_agg(CASE WHEN first_loan_cash is not null then struct(ocurred_on, first_loan_cash) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_loan_cash as first_loan_cash,
    element_at(array_sort(array_agg(CASE WHEN first_loan_npv is not null then struct(ocurred_on, first_loan_npv) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_loan_npv as first_loan_npv,
    element_at(array_sort(array_agg(CASE WHEN first_loan_roe is not null then struct(ocurred_on, first_loan_roe) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).first_loan_roe as first_loan_roe,
    element_at(array_sort(array_agg(CASE WHEN ia_pd_multiplier is not null then struct(ocurred_on, ia_pd_multiplier) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).ia_pd_multiplier as ia_pd_multiplier,
    element_at(array_sort(array_agg(CASE WHEN interest_rate is not null then struct(ocurred_on, interest_rate) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).interest_rate as interest_rate,
    element_at(array_sort(array_agg(CASE WHEN lbl is not null then struct(ocurred_on, lbl) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lbl as lbl,
    element_at(array_sort(array_agg(CASE WHEN learning_population is not null then struct(ocurred_on, learning_population) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).learning_population as learning_population,
    element_at(array_sort(array_agg(CASE WHEN lifetime_cash is not null then struct(ocurred_on, lifetime_cash) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lifetime_cash as lifetime_cash,
    element_at(array_sort(array_agg(CASE WHEN lifetime_npv is not null then struct(ocurred_on, lifetime_npv) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lifetime_npv as lifetime_npv,
    element_at(array_sort(array_agg(CASE WHEN lifetime_roe is not null then struct(ocurred_on, lifetime_roe) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).lifetime_roe as lifetime_roe,
    element_at(array_sort(array_agg(CASE WHEN returning_client is not null then struct(ocurred_on, returning_client) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).returning_client as returning_client,
    element_at(array_sort(array_agg(CASE WHEN term is not null then struct(ocurred_on, term) else NULL end), (left, right) -> case when left.ocurred_on < right.ocurred_on then 1 when left.ocurred_on > right.ocurred_on then -1 when left.ocurred_on == right.ocurred_on then 0 end), 1).term as term,
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
this: silver.f_loan_proposals_br
country: br
silver_table_name: f_loan_proposals_br
table_pk_fields: ['loan_proposal_id']
table_pk_amount: 1
fields_direct: ['ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'application_id', 'approved_amount', 'client_id', 'contribution_margin', 'decision_npv', 'discount_rate', 'first_loan_cash', 'first_loan_npv', 'first_loan_roe', 'ia_pd_multiplier', 'interest_rate', 'lbl', 'learning_population', 'lifetime_cash', 'lifetime_npv', 'lifetime_roe', 'loan_proposal_id', 'ocurred_on', 'returning_client', 'term', 'total_interest']
mandatory_fields: ['event_name', 'event_id']
events_dict: {'conditionalcreditcheckpassed_unnested_by_loan_proposals_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'creditcheckpassed_unnested_by_loan_proposals_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckpassed_unnested_by_loan_proposal_event': {'stage': 'legacy_or_non_origination', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'conditionalcreditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'creditcheckpassedbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'creditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckpassedbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_rc_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'psychometricevaluationisrequiredbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'creditcheckfailedbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'clientcreditcheckfailedbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_rc_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_mdf_cancellation', 'ally_mdf_fraud', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'decision_npv', 'first_loan_npv', 'lifetime_npv', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'ia_pd_multiplier', 'total_interest'], 'custom_attributes': {}}, 'preapprovalconditionalcreditcheckpassedbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'preapprovalcreditcheckfailedbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}, 'preapprovalcreditcheckpassedbr_unnested_by_loan_proposals_event': {'stage': 'underwriting_preapproval_br', 'direct_attributes': ['loan_proposal_id', 'client_id', 'application_id', 'ocurred_on', 'term', 'ally_mdf', 'ally_slug', 'interest_rate', 'returning_client', 'lbl', 'learning_population', 'discount_rate', 'first_loan_roe', 'first_loan_cash', 'lifetime_roe', 'lifetime_cash', 'contribution_margin', 'approved_amount', 'total_interest'], 'custom_attributes': {}}}
events_keys: ['conditionalcreditcheckpassed_unnested_by_loan_proposals_event', 'creditcheckpassed_unnested_by_loan_proposals_event', 'clientcreditcheckpassed_unnested_by_loan_proposal_event', 'conditionalcreditcheckpassedbr_unnested_by_loan_proposals_event', 'conditionalcreditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event', 'creditcheckpassedbr_unnested_by_loan_proposals_event', 'creditcheckpassedwithlowerapprovedamountbr_unnested_by_loan_proposals_event', 'clientcreditcheckpassedbr_unnested_by_loan_proposals_event', 'psychometricevaluationisrequiredbr_unnested_by_loan_proposals_event', 'creditcheckfailedbr_unnested_by_loan_proposals_event', 'clientcreditcheckfailedbr_unnested_by_loan_proposals_event', 'preapprovalconditionalcreditcheckpassedbr_unnested_by_loan_proposals_event', 'preapprovalcreditcheckfailedbr_unnested_by_loan_proposals_event', 'preapprovalcreditcheckpassedbr_unnested_by_loan_proposals_event']
flag_group_feature_active: True
version: silver_sql_builder_alternative
*/
