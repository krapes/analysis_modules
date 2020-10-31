WITH count_steps AS (
SELECT
        TimeStamp,
        SessionId,
        FlowName
FROM `cosmic-octane-88917.analytics_us._VW_ApplicationExitEvent`
),

calc_values AS (
SELECT
      EXTRACT(DATE FROM TimeStamp) AS date,
      COUNT(DISTINCT(SessionId)) AS count,
      FlowName
FROM count_steps
WHERE FlowName in {0}
GROUP BY date, FlowName

)

SELECT *,
      AVG(count) OVER(PARTITION BY FlowName ORDER BY date
                 ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS avg_14_day_count
FROM calc_values