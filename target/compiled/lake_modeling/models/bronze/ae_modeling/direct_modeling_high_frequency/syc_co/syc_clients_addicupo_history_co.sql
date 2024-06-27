

--raw.syc_clients_addicupo_history_co
SELECT
    -- DIRECT MODELING FIELDS
    id as id,
    client_id,
    CAST(total_addicupo AS DECIMAL) as total_addicupo,
    update_addicupo_reason,
    addicupo_state,
    to_date(addicupo_last_update) as addicupo_last_update,
    update_addicupo_source,
    CAST(static_addicupo AS BOOLEAN) as static_addicupo,
    CAST(remaining_addicupo AS DECIMAL) as remaining_addicupo,
    addicupo_state_v2,
    addicupo_state_v2_reason,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('2022-01-01') AS updated_at
-- DBT SOURCE REFERENCE
from raw.syc_clients_addicupo_history_co