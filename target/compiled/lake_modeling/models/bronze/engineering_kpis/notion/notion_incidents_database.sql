

--raw.notion_page
SELECT DISTINCT
    _airbyte_data.archived AS archived,
    _airbyte_data.created_time AS created_time,
    _airbyte_data.id AS id,
    _airbyte_data.properties[4].value.checkbox AS autodetected,
    _airbyte_data.properties[10].value.date.start AS incident_date,
    _airbyte_data.properties[11].value.number AS gmv_impact_br,
    _airbyte_data.properties[1].value.number AS gmv_impact_co,
    _airbyte_data.properties[18].value.title.text.content AS incident_name,
    _airbyte_data.properties[2].value.multi_select.name AS incident_owner,
    _airbyte_data.properties[0].value.select.name AS priority,
    _airbyte_data.properties[12].value.select.name AS root_cause,
    _airbyte_data.properties[13].value.select.name AS severity,
    _airbyte_data.properties[6].value.checkbox AS socialized_postmortem,
    _airbyte_data.properties[5].value.select.name AS status, 
    _airbyte_data.properties[3].value.number AS ttd,
    _airbyte_data.properties[15].value.number AS ttr,
    _airbyte_data.properties[17].value.select.name AS urgency,
    _airbyte_data.url AS incident_url
FROM raw.notion_page
where _airbyte_data.parent.database_id = '3601eaf2-9e32-459e-85db-55aaae5568a2'