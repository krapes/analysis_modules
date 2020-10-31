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
        major_events
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
      WHERE FlowName in {0}
      AND EXTRACT(DATE FROM TimeStamp) BETWEEN '2020-06-01' AND '2020-10-31'
      GROUP BY Path
      ORDER BY count DESC
      LIMIT 10
      )
)

SELECT DISTINCT SessionId
FROM Session_paths sess
INNER JOIN top_10 top
USING(Path)
WHERE top.nickname = '{1}'