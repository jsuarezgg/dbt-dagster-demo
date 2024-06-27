{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

SELECT
    c.contact_id,
    c.contact_created_date,
    c.contact_name,
    c.contact_email,
    c.contact_has_opted_out_of_email,
    c.contact_is_email_bounced,
    c.contact_phone,
    c.contact_mpbile_phone,
    c.contact_formatted_phone_number,
    c.contact_mailing_city,
    c.contact_id_type,
    c.contact_id_number,
    c.contact_sector_del_comercio_aliados,
    c.contact_ventas_mensuales,
    c.contact_applciation_url,
    c.contact_application_status,
    c.contact_latest_source,
    c.contact_first_referring_site,
    c.contact_original_source,
    c.contact_original_source_drill_down_1,
    c.contact_original_source_drill_down_2,
    c.contact_is_deleted,
    c.contact_owner_id,
    c.contact_account_id,
    -- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    c.airbyte_emitted_at,
    c.ingested_from_s3_at
from {{ ref('salesforce_contact')}} c