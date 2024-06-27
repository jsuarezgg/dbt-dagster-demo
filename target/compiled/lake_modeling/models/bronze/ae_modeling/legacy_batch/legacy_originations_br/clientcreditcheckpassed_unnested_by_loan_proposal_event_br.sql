


--raw_modeling.clientcreditcheckpassed_br
WITH select_explode AS (
    SELECT
        -- MANDATORY FIELDS
        json_tmp.eventType AS event_name_original,
        reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
        json_tmp.eventId AS event_id,
        CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
        to_date(json_tmp.ocurredOn) AS ocurred_on_date,
        NOW() AS ingested_at,
        to_timestamp('2022-01-01') AS updated_at,
        -- MAPPED FIELDS - DIRECT ATTRIBUTES
        coalesce(json_tmp.clientLoanApplication.allyId.slug,
                json_tmp.metadata.context.allyId) as ally_slug,
        coalesce(json_tmp.clientLoanApplication.applicationId.id,
                json_tmp.clientLoanApplication.applicationId.value,
                json_tmp.metadata.context.traceId) as application_id,
        coalesce(json_tmp.clientLoanApplication.client.id.id,
                json_tmp.metadata.context.clientId) as client_id,
        json_tmp.lowBalanceLoanV2.value as lbl,
        json_tmp.returningClient as returning_client,
        EXPLODE(json_tmp.loanProposals) as loan_proposals_event
        -- CUSTOM ATTRIBUTES
    -- DBT SOURCE REFERENCE
    from raw_modeling.clientcreditcheckpassed_br
    -- DBT INCREMENTAL SENTENCE


    WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
    AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")


)

SELECT 
    CONCAT('EID_',event_id,'_LPEID_',loan_proposals_event.id.id) AS surrogate_key,
	event_name_original,
	event_name,
	event_id,
	ocurred_on,
	ocurred_on_date,
	ingested_at,
	updated_at,
	ally_slug,
    application_id,
	client_id,
    lbl,
    returning_client,
	loan_proposals_event.allyDiscountFee as ally_mdf,
    loan_proposals_event.approvedAmount as approved_amount,
    loan_proposals_event.contributionMargin as contribution_margin,
    loan_proposals_event.decisionNPV as decision_npv,
    loan_proposals_event.decisionDiscountRate as discount_rate,
    loan_proposals_event.firstLoanCash.value as first_loan_cash,
    loan_proposals_event.reportingNPV as first_loan_npv,
    loan_proposals_event.firstLoanROE.value as first_loan_roe,
    loan_proposals_event.effectiveAnnualRate as interest_rate,
    loan_proposals_event.learningPopulation as learning_population,
    loan_proposals_event.lifetimeCash.value as lifetime_cash,
    loan_proposals_event.repeatingNPV as lifetime_npv,
    loan_proposals_event.lifetimeROE.value as lifetime_roe,
    loan_proposals_event.id.id as loan_proposal_id,
    loan_proposals_event.term as term,
    loan_proposals_event.totalInterest as total_interest
FROM select_explode