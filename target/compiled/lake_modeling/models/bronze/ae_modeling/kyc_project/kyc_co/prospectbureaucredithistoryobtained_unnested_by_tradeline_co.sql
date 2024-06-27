
--raw_modeling.prospectbureaucredithistoryobtained_co
WITH select_explode AS (
    SELECT
        -- MANDATORY FIELDS
        json_tmp.eventType AS event_name_original,
        reverse(split(json_tmp.eventType,"\\."))[0] AS event_name,
        json_tmp.eventId AS event_id,
        CAST(ocurred_on AS TIMESTAMP) AS ocurred_on,
        dt AS ocurred_on_date,
        NOW() AS ingested_at,
        to_timestamp('2022-01-01') AS updated_at,
        -- MAPPED FIELDS
        json_tmp.applicationId AS application_id,
        json_tmp.prospectId AS client_id,
        EXPLODE(json_tmp.commercial.tradelines) AS item
        -- CUSTOM ATTRIBUTES
        -- CAST(ocurred_on AS TIMESTAMP) AS prospectbureaucredithistoryobtained_co_at -- To store it as a standalone column, when needed
    FROM  raw_modeling.prospectbureaucredithistoryobtained_co
    -- DBT INCREMENTAL SENTENCE

    
        WHERE dt BETWEEN (to_date("2022-01-01"- INTERVAL "10" HOUR)) AND to_date("2022-01-30")
        AND CAST(ocurred_on AS TIMESTAMP) BETWEEN (to_timestamp("2022-01-01"- INTERVAL "10" HOUR)) AND to_timestamp("2022-01-30")
    

)
SELECT
    -- ITEM FIELDS
    CONCAT('EID_',event_id,'_IPI_',ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A')) AS surrogate_key,
    ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY 'A') AS item_pseudo_idx,
    -- MANDATORY FIELDS
    event_name_original,
    event_name,
    event_id,
    application_id,
    client_id,
    'json_tmp.commercial.tradelines._ARRAY_' AS array_parent_path,
    ocurred_on,
    ocurred_on_date,
    ingested_at,
    updated_at,
    -- SPECIAL DOUBLE ARRAYS - ITEM ARRAYS
    item.pastMonthlyBehaviours AS bureau_tradeline_pastMonthlyBehaviours,
    -- ITEM INNER FIELDS
    item.balance AS bureau_tradeline_balance,
    item.balanceShare AS bureau_tradeline_balanceShare,
    item.branch AS bureau_tradeline_branch,
    item.city AS bureau_tradeline_city,
    item.clauseNumberMonths AS bureau_tradeline_clauseNumberMonths,
    item.contractNumberMonths AS bureau_tradeline_contractNumberMonths,
    item.contractType AS bureau_tradeline_contractType,
    item.creditCardClass AS bureau_tradeline_creditCardClass,
    item.creditLineType AS bureau_tradeline_creditLineType,
    item.creditType AS bureau_tradeline_creditType,
    item.currency AS bureau_tradeline_currency,
    item.debtorStatus AS bureau_tradeline_debtorStatus,
    item.debtorType AS bureau_tradeline_debtorType,
    item.delinquencyLikelihood AS bureau_tradeline_delinquencyLikelihood,
    item.entityName AS bureau_tradeline_entityName,
    item.entityType AS bureau_tradeline_entityType,
    item.extinctionPaymentMode AS bureau_tradeline_extinctionPaymentMode,
    item.franchise AS bureau_tradeline_franchise,
    item.initialApprovedAmount AS bureau_tradeline_initialApprovedAmount,
    item.installmentAmount AS bureau_tradeline_installmentAmount,
    item.installmentsPaid AS bureau_tradeline_installmentsPaid,
    TO_TIMESTAMP(item.lastUpdateDate) AS bureau_tradeline_lastUpdateDate,
    item.maxDaysPastDue AS bureau_tradeline_maxDaysPastDue,
    item.minMonthsClause AS bureau_tradeline_minMonthsClause,
    item.negativeInfoVisibleUntil AS bureau_tradeline_negativeInfoVisibleUntil,
    item.numberAgreedInstallments AS bureau_tradeline_numberAgreedInstallments,
    item.numberInstallmentsPastDue AS bureau_tradeline_numberInstallmentsPastDue,
    item.obligationNumber AS bureau_tradeline_obligationNumber,
    item.obligationStatus AS bureau_tradeline_obligationStatus,
    TO_TIMESTAMP(item.obligationStatusLastUpdateDate) AS bureau_tradeline_obligationStatusLastUpdateDate,
    item.obligationType AS bureau_tradeline_obligationType,
    TO_TIMESTAMP(item.originationDate) AS bureau_tradeline_originationDate,
    item.originationEntityName AS bureau_tradeline_originationEntityName,
    item.pastDueAmount AS bureau_tradeline_pastDueAmount,
    item.pastMonthlyBehaviour AS bureau_tradeline_pastMonthlyBehaviour,
    TO_TIMESTAMP(item.paymentDate) AS bureau_tradeline_paymentDate,
    item.paymentStatus AS bureau_tradeline_paymentStatus,
    TO_TIMESTAMP(item.paymentStatusLastUpdateDate) AS bureau_tradeline_paymentStatusLastUpdateDate,
    item.paymentType AS bureau_tradeline_paymentType,
    item.periodicity AS bureau_tradeline_periodicity,
    item.plasticStatus AS bureau_tradeline_plasticStatus,
    TO_TIMESTAMP(item.plasticStatusLastUpdateDate) AS bureau_tradeline_plasticStatusLastUpdateDate,
    item.rating AS bureau_tradeline_rating,
    item.refinanceType AS bureau_tradeline_refinanceType,
    item.refinanced AS bureau_tradeline_refinanced,
    TO_TIMESTAMP(item.refinancedDate) AS bureau_tradeline_refinancedDate,
    item.returnedChecks AS bureau_tradeline_returnedChecks,
    TO_TIMESTAMP(item.terminationDate) AS bureau_tradeline_terminationDate,
    item.timesRefinanced AS bureau_tradeline_timesRefinanced,
    item.validity AS bureau_tradeline_validity,
    item.warrantyCoverage AS bureau_tradeline_warrantyCoverage,
    item.warrantyType AS bureau_tradeline_warrantyType

FROM select_explode