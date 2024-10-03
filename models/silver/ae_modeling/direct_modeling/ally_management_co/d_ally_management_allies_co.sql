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
    -- MANDATORY FIELDS
    -- MAPPED FIELDS - DIRECT ATTRIBUTES
    ally_slug,
    ally_name,
    vertical:['name']:['value'] vertical_name,
    vertical:['slug']:['value'] vertical_slug,
    vertical:['isNew']:['value'] vertical_isnew,
    brand:['name']:['value'] brand_name,
    brand:['slug']:['value'] brand_slug,
    website,
    type,
    categories:[0]:['value'] category,
    from_json(categories_v2, 'array<struct<slug: STRING>>').slug AS category_v2,
    from_json(categories_v2, 'array<struct<subCategories: STRING>>').subCategories AS category_v2_subcategory,
    active_ally,
    channel,
    from_json(tags, 'array<struct<value: STRING>>').value AS tags,
    ally_state,
    description,
    commercial_type,
    additional_information:['address']:['lineOne'] AS address_lineone,
    additional_information:['address']:['postalCode'] AS address_postalcode,
    additional_information:['address']:['city'] AS address_city,
    additional_information:['address']:['state'] AS address_state,
    additional_information:['legalIdentification']['name'] AS legal_representative_name,
    additional_information:['legalIdentification']['type'] AS legal_representative_id_type,
    additional_information:['legalIdentification']['number'] AS legal_representative_id_number,
    additional_information:['financialInformation']['name'] AS financial_representative_name,
    additional_information:['financialInformation']['type'] AS financial_representative_id_type,
    additional_information:['financialInformation']['number'] AS financial_representative_id_number,
    additional_information:['finalBeneficiaries'][0]['lastName'] as first_final_beneficiaries_last_name,
    additional_information:['finalBeneficiaries'][0]['firstName'] as first_final_beneficiaries_first_name,
    additional_information:['finalBeneficiaries'][0]['idType'] as first_final_beneficiaries_id_type,
    additional_information:['finalBeneficiaries'][0]['idNumber'] as first_final_beneficiaries_id_number,
    -- CUSTOM ATTRIBUTES
    NOW() AS ingested_at,
    to_timestamp('{{ var("execution_date") }}') AS updated_at
-- DBT SOURCE REFERENCE
FROM {{ ref('ally_management_allies_co') }}