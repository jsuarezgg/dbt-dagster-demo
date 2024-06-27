

--raw.client_management_reimbursements_br
SELECT
    -- MANDATORY FIELDS
    id AS reimbursement_id,
    client_id,
    reimbursement_date,
    amount,
    created_at,
    annulled,
    annulment_reason,
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at

-- DBT SOURCE REFERENCE
FROM raw.client_management_reimbursements_br