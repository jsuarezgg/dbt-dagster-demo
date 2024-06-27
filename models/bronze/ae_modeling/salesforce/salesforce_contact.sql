{{
    config(
        materialized='table',
        full_refresh = false,
        post_hook=[
            'ANALYZE TABLE {{ this }} COMPUTE STATISTICS',
        ]
    )
}}

--raw.salesforce_contact_full_refresh_overwrite
SELECT
	Id AS contact_id,
	CreatedDate.member0 AS contact_created_date,
	Name AS contact_name,
	Email AS contact_email,
	HasOptedOutOfEmail AS contact_has_opted_out_of_email,
	IsEmailBounced AS contact_is_email_bounced,
	Phone AS contact_phone,
	MobilePhone AS contact_mpbile_phone,
	Formatted_Phone_Number__c AS contact_formatted_phone_number,
	MailingCity AS contact_mailing_city,
	ID_type__c AS contact_id_type,
	ID_Number__c AS contact_id_number,
	Sector_del_Comercio_Aliados__c AS contact_sector_del_comercio_aliados,
	Ventas_Mensuales__c AS contact_ventas_mensuales,
	Application_URL__c AS contact_applciation_url,
	Application_Status__c AS contact_application_status,
	Latest_Source__c AS contact_latest_source,
	First_Referring_Site__c AS contact_first_referring_site,
	Original_Source__c AS contact_original_source,
	Original_Source_Drill_Down_1__c AS contact_original_source_drill_down_1,
	Original_Source_Drill_Down_2__c AS contact_original_source_drill_down_2,
	IsDeleted AS contact_is_deleted,
	OwnerId AS contact_owner_id,
	AccountId AS contact_account_id,
	-- MANDATORY FIELDS
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at,
    `_airbyte_emitted_at` AS airbyte_emitted_at,
    ingested_from_s3_at
    -- CUSTOM ATTRIBUTES
    -- Fill with your custom attributes
-- DBT SOURCE REFERENCE
from {{ source('raw', 'salesforce_contact_full_refresh_overwrite') }}



