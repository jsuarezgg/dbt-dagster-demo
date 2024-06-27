

WITH ignored_convo_by_custom_attribute(
	SELECT resourceId AS conversationId,
		   1 AS ignore_flag
	FROM cur.kus_custom_attributes
	WHERE RTRIM(LTRIM(customAttributeName)) IN ('motivoDeContatoStr','motivoDeContato2Str')
		AND customAttributeValue IN ('Abandono','Teste')
		AND resourceName ='conversations'
	GROUP BY 1,2
)

SELECT 
    'BR' AS country_code,
	c.conversationId,
	q.name AS last_queue_name,
	--cfmi.direction AS direction, -- alternative recalculated version
    from_utc_timestamp(c.createdAt,'America/Sao_Paulo') AS createdAt_local,
    to_date(from_utc_timestamp(c.createdAt,'America/Sao_Paulo')) AS createdAt_date_local,
	c.createdAt,
	c.referenceDate AS referenceDate,
	c.name,
	c.preview,
	c.channels,
	c.status,
	c.ended,
	c.endedById,
/*	CASE WHEN c.endedById IS NULL THEN ARRAY() ELSE ARRAY(c.endedById) END AS endedById,
	transform(CASE WHEN c.endedById IS NULL THEN ARRAY() ELSE ARRAY(c.endedById) END,
			  x-> element_at(mf.map_user,x)) AS endedByIdName,*/
	c.messageCount,
	c.noteCount,
	--c.satisfaction,
	c.satisfactionLvl_Channel,
	c.satisfactionLvl_Status,
	c.satisfactionLvl_ScheduledFor,
	c.satisfactionLvl_CreatedAt,
	c.satisfactionLvl_SentAt,
	c.satisfactionLvl_Rating,
	c.satisfactionLvl_Score,
    c.satisfactionLvl_FirstAnswer,
    c.satisfactionLvl_SentBy,
/*	CASE WHEN c.satisfactionLvl_SentBy IS NULL THEN ARRAY() ELSE ARRAY(c.satisfactionLvl_SentBy) END AS satisfactionLvl_SentBy,
	transform(CASE WHEN c.satisfactionLvl_SentBy IS NULL THEN ARRAY() ELSE ARRAY(c.satisfactionLvl_SentBy) END,
			  x-> element_at(mf.map_user,x)) AS satisfactionLvl_SentByName,
	cfmi.messageId AS firstMessage_Id, 
	cfmi.directionType AS firstMessage_DirectionType,
	cfmi.sentAt AS firstMessage_sentAt,
	c.externalQueue,*/
	c.customerId,
	c.queueId AS last_queueId
	/*,
	ciut.involvedUsers,
	ciut.involvedTeams,
	ciut.involvedUsersNames,
	ciut.involvedTeamsNames,
	CASE WHEN arrays_overlap(ciut.involvedTeamsNames,array('PLACEHOLDER TO BE REPLACED BY LIST IF IDS - change variable for their id counterpart')) THEN 'Brazil'
		 WHEN arrays_overlap(ciut.involvedTeamsNames,array('PLACEHOLDER TO BE REPLACED BY LIST IF IDS - change variable for their id counterpart')) THEN 'Colombia'
		 ELSE 'Not CS team'
	END AS country_involvedTeams,
	CARDINALITY(ciut.involvedUsersNames) AS num_involvedUsers*/
	
FROM      cur.kus_conversations                 AS c
LEFT JOIN cur.kus_queues                        AS q    ON c.queueId = q.queueId 
LEFT JOIN ignored_convo_by_custom_attribute     AS icbca ON c.conversationId = icbca.conversationId
/*
LEFT JOIN mapping_functions                     AS mf   ON 1=1
LEFT JOIN conversation_involved_users_and_teams AS ciut ON ciut.conversationId = c.conversationId
LEFT JOIN conversation_first_message_info       AS cfmi ON cfmi.conversationId = c.conversationId
*/

WHERE 1=1
	AND COALESCE(icbca.ignore_flag,0)= 0
	AND c.queueId = '6021aa072f60d00019a63064' --KEY FILTER Criteria by business. Right now this ID corresponds to the queue name: 'BR - Atendimento Clientes'
	AND c.satisfactionLvl_Status IS NOT NULL --ONLY conversations with CSAT (even unresponded)
	-- AND cfmi.direction = 'in' --Recalculated first message direction  
	AND c.createdAt > '2021-06-15'
	--AND c.conversationId = '60e4c38591df4511b5b384a9' -- '60e4656b4ca099caaf55959d' -- Testing cases
	AND c.messageCount >=1 --Ignore empty convos
	/*AND cfmi.directionType = 'initial-in'
	AND arrays_overlap(ciut.involvedTeamsNames,
					   array('PLACEHOLDER TO BE REPLACED BY LIST IF IDS - change variable for their id counterpart')
					  )
ORDER BY cfmi.sentAt DESC)*/