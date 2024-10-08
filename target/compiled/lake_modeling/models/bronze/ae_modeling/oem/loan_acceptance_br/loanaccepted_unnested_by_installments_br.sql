


--raw_modeling.loanacceptedbr_br
WITH select_explode as (
    SELECT
        -- MANDATORY FIELDS
        json_tmp.eventType AS event_name_original,
        reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
        json_tmp.eventId AS event_id,
        CAST(json_tmp.ocurredOn AS TIMESTAMP) AS ocurred_on,
        to_date(json_tmp.ocurredOn) AS ocurred_on_date,
        NOW() AS ingested_at,
        to_timestamp('2022-01-01') AS updated_at,
        -- MAPPED FIELDS - DIRECT ATTRIBUTES (CDA:SECTION VERIFIED AUTOMATICALLY)
        json_tmp.application.id AS application_id,
        json_tmp.client.id AS client_id,
        json_tmp.client.type AS client_type,
        json_tmp.originationEventType AS event_type,
        json_tmp.metadata.context.allyId AS ally_slug,
        json_tmp.application.journey.name AS journey_name,
        json_tmp.application.journey.currentStage.name AS journey_stage_name,
        json_tmp.application.channel AS channel,
        json_tmp.application.product AS product,
        EXPLODE(json_tmp.loan.installments) as installment
        -- CUSTOM ATTRIBUTES
        -- Fill with your custom attributes
        -- CAST(ocurred_on AS TIMESTAMP) AS loanacceptedbr_br_at -- To store it as a standalone column, when needed
    -- DBT SOURCE REFERENCE
    from raw_modeling.loanacceptedbr_br
    -- DBT INCREMENTAL SENTENCE

    
        WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
        AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
    
)

SELECT
    CONCAT('EID_',event_id,'_IPI_',row_number() OVER (partition by event_id order by ocurred_on)) AS surrogate_key_installment,
    event_name_original,
    event_name,
    event_id,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    application_id,
    client_id,
    client_type,
    event_type,
    ally_slug,
    journey_name,
    journey_stage_name,
    channel,
    product,
    installment.dueDate as installment_due_date,
    installment.interest as installment_interest,
    installment.pmt as installment_pmt,
    installment.principal as installment_principal

FROM select_explode