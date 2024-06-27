

--bronze.syc_clients_br
SELECT
    -- DIRECT MODELING FIELDS
    client_id,
    total_addicupo,
    remaining_addicupo,
    addicupo_state,
    addicupo_last_update,
    static_addicupo,
    last_update_addicupo_reason,
    last_update_addicupo_source,
    addicupo_state_v2,
    addicupo_state_v2_reason,
    ignore_progression,
    initial_addicupo,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from bronze.syc_clients_br