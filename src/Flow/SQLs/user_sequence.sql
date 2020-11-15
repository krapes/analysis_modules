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
),


Session_paths AS (
SELECT DISTINCT SessionId, TimeStamp, FlowName,  Duration, Path
FROM (
    SELECT
      SessionId,
      FlowName,
      FIRST_VALUE(TimeStamp) OVER(PARTITION BY SessionId, FlowName ORDER BY TimeStamp) AS TimeStamp,
      TIMESTAMP_DIFF(TimeStamp, FIRST_VALUE(TimeStamp) OVER(PARTITION BY SessionId, FlowName ORDER BY TimeStamp), SECOND) AS Duration,
      STRING_AGG(ActionId, ';') OVER(PARTITION BY SessionId, FlowName ORDER BY TimeStamp) AS Path,
      RANK() OVER(PARTITION BY SessionId, FlowName ORDER BY TimeStamp DESC) AS rank
    FROM
        filtered
       )
WHERE rank = 1
),

top_10 AS (
SELECT *,
      CONCAT(ROW_NUMBER() OVER(), '-Path_Freq_Rank') AS nickname
FROM (
      SELECT
        Path,
        COUNT(TimeStamp) count
      FROM Session_paths
      GROUP BY Path
      ORDER BY count DESC
      )
)

SELECT
       m.SessionId AS user_id,
       m.ActionId  AS event_name,
       m.TimeStamp AS time_event,
       m.rank_event,
       m.next_event,
       m.FlowName,
       TIMESTAMP_DIFF(m.TimeStamp, m.first_value, SECOND) AS time_from_start,
       TIMESTAMP_DIFF(m.next_value, m.TimeStamp, SECOND) AS time_to_next,
       t.nickname AS path_nickname,
       1 AS count
FROM metric_prep m
INNER JOIN Session_paths USING(SessionId)
INNER JOIN top_10 t USING(Path)
ORDER BY user_id, time_event


