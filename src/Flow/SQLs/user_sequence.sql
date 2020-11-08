WITH major_events AS (
SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', 'API_EVENT') AS ActionId,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_ApiEvent`

UNION ALL

SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', 'CALL_EVENT', ':', CallAnswerIndicator) AS ActionId,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_CallEvent`
WHERE ActionId != 'TRANSFER_33'

UNION ALL

SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', CAST(DtmfInput AS STRING), '-dtmf') ActionId,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_DtmfEvent`

UNION ALL

SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', 'MSG_EVENT') AS ActionId,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_MsgEvent`

UNION ALL

SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', 'NLP_EVENT') AS ActionId,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_NlpEvent`
),

filtered AS (
SELECT  SessionId,
        TimeStamp,
        ActionId,
        FlowName
FROM major_events
WHERE
FlowName in {0}
AND EXTRACT(DATE FROM TimeStamp) BETWEEN '{1}' AND '{2}'
),

metric_prep AS (
SELECT
       SessionId,
       ActionId,
       TimeStamp,
       ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY TimeStamp) AS rank_event,
       FIRST_VALUE(TimeStamp) OVER (PARTITION BY SessionId ORDER BY TimeStamp) AS first_value,
       LEAD(TimeStamp) OVER (PARTITION BY SessionId ORDER BY TimeStamp) AS next_value,
       LEAD(ActionId) OVER (PARTITION BY SessionId ORDER BY TimeStamp) AS next_event,
       FlowName,

FROM
filtered
)

SELECT
       SessionId AS user_id,
       ActionId  AS event_name,
       TimeStamp AS time_event,
       rank_event,
       next_event,
       FlowName,
       TIMESTAMP_DIFF(TimeStamp, first_value, SECOND) AS time_from_start,
       TIMESTAMP_DIFF(next_value, TimeStamp, SECOND) AS time_to_next
FROM metric_prep
ORDER BY user_id, time_event
