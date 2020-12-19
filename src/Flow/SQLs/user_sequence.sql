WITH major_events AS (
SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', 'API_EVENT') AS ActionId,
      NULL AS CallingNumber,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_ApiEvent`

UNION ALL

SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', 'CALL_EVENT', ':', CallAnswerIndicator) AS ActionId,
      CallingNumber,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_CallEvent`
WHERE ActionId != 'TRANSFER_33'

UNION ALL

SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', CAST(DtmfInput AS STRING), '-dtmf') ActionId,
      NULL AS CallingNumber,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_DtmfEvent`

UNION ALL

SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', 'MSG_EVENT') AS ActionId,
      NULL AS CallingNumber,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_MsgEvent`

UNION ALL

SELECT
      SessionId,
      TimeStamp,
      CONCAT(ActionId, '~~', 'NLP_EVENT') AS ActionId,
      NULL AS CallingNumber,
      CustomerId,
      FlowName
FROM `cosmic-octane-88917.analytics_us._VW_NlpEvent`
),

filtered AS (
SELECT  SessionId,
        TimeStamp,
        ActionId,
        CallingNumber,
        FlowName
FROM major_events
WHERE FlowName in {0}
AND EXTRACT(DATE FROM TimeStamp) BETWEEN '{1}' AND '{2}'
--LIMIT 100000
),

callback_subset AS (
SELECT DISTINCT *,
        RANK() OVER(PARTITION BY CallingNumber ORDER BY TimeStamp) rank
FROM (
        SELECT DISTINCT CallingNumber, SessionId, MIN(Timestamp) TimeStamp
        FROM filtered f
        WHERE CallingNumber != 'Restricted'
        AND CallingNumber IS NOT NULL
        GROUP BY CallingNumber, SessionId
     )
),


metric_prep AS (
SELECT
       SessionId,
       ActionId,
       TimeStamp,
       ROW_NUMBER() OVER (PARTITION BY SessionId ORDER BY TimeStamp) AS rank_event,
       FIRST_VALUE(TimeStamp) OVER (PARTITION BY SessionId ORDER BY TimeStamp) AS first_timestamp,
       LEAD(TimeStamp) OVER (PARTITION BY SessionId ORDER BY TimeStamp) AS next_timestamp,
       LEAD(ActionId) OVER (PARTITION BY SessionId ORDER BY TimeStamp) AS next_event,
       FlowName,

FROM
filtered
),


Session_paths AS (
SELECT DISTINCT SessionId, TimeStamp, Path, session_duration
FROM (
    SELECT
      SessionId,
      --FlowName,
      FIRST_VALUE(TimeStamp) OVER(PARTITION BY SessionId ORDER BY TimeStamp) AS TimeStamp,
      TIMESTAMP_DIFF(TimeStamp, FIRST_VALUE(TimeStamp) OVER(PARTITION BY SessionId ORDER BY TimeStamp), SECOND) AS session_duration,
      STRING_AGG(ActionId, ';') OVER(PARTITION BY SessionId ORDER BY TimeStamp) AS Path,
      RANK() OVER(PARTITION BY SessionId ORDER BY TimeStamp DESC) AS rank
    FROM
        filtered
       )
WHERE rank = 1
),

path_ranks AS (
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
       DISTINCT
       m.SessionId AS user_id,
       m.ActionId  AS event_name,
       m.TimeStamp AS time_event,
       CAST(EXTRACT(DATE FROM m.TimeStamp) AS DATETIME) AS date,
       m.rank_event AS rank_event,
       m.next_event AS next_event,
       m.FlowName AS FlowName,
       TIMESTAMP_DIFF(m.TimeStamp, m.first_timestamp, SECOND) AS time_from_start,
       TIMESTAMP_DIFF(m.next_timestamp, m.TimeStamp, SECOND) AS time_to_next,
       pr.nickname AS path_nickname,
       s.session_duration,
       1 AS count,
       cb.CallingNumber,
       cb.rank callback_instance
FROM metric_prep m
INNER JOIN Session_paths s USING(SessionId)
INNER JOIN path_ranks pr USING(Path)
LEFT JOIN callback_subset cb USING(SessionId)
ORDER BY user_id, time_event







