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

callback_subset AS (
SELECT DISTINCT  c.*
FROM filtered f
INNER JOIN `cosmic-octane-88917.analytics_us._VW_CallEvent` c
USING(SessionId)
WHERE c.CallingNumber != 'Restricted'
),

callbacks AS (
SELECT DISTINCT CallingNumber,
      /*
       CASE WHEN CallingNumber like '+1800%' THEN True ELSE False END AS TollFreeNumber,
       EXTRACT(MONTH FROM c1.TimeStamp) month,
       c1.TimeStamp,
       */
       c1.SessionId first_session_id,
       -- c1.CallAnswerIndicator,
       -- c1.CallDuration first_call_duration,
       -- c2.TimeStamp,
       c2.SessionId second_session_id,
       /*
       c2.CallAnswerIndicator,
       c2.CallDuration second_call_duration,
       ABS(TIMESTAMP_DIFF(c1.TimeStamp, c2.TimeStamp, DAY)) day_diff
       */
FROM callback_subset c1
INNER JOIN callback_subset c2
USING(CallingNumber)
WHERE c1.SessionId != c2.SessionId
AND c1.TimeStamp < c2.TimeStamp
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
),

final AS (
SELECT
       m.SessionId AS user_id,
       m.ActionId  AS event_name,
       m.TimeStamp AS time_event,
       m.rank_event AS rank_event,
       m.next_event AS next_event,
       m.FlowName AS FlowName,
       TIMESTAMP_DIFF(m.TimeStamp, m.first_value, SECOND) AS time_from_start,
       TIMESTAMP_DIFF(m.next_value, m.TimeStamp, SECOND) AS time_to_next,
       t.nickname AS path_nickname,
       1 AS count,
       CASE WHEN cb1.first_session_id IS NOT NULL THEN 1
            WHEN cb2.second_session_id IS NOT NULL THEN 2
       ELSE NULL END AS callback_route
FROM metric_prep m
INNER JOIN Session_paths USING(SessionId)
INNER JOIN top_10 t USING(Path)
LEFT JOIN callbacks cb1 ON m.SessionId=cb1.first_session_id
LEFT JOIN callbacks cb2 ON m.SessionId=cb2.second_session_id
ORDER BY user_id, time_event
)

SELECT callback_route, count(DISTINCT user_id)
FROM final
GROUP BY callback_route


