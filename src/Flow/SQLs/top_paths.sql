WITH count_steps AS (
SELECT
        TimeStamp,
        SessionId,
        EndTime - StartTime AS Duration,
        FlowName,
        Path,
        CHARACTER_LENGTH(Path) - CHARACTER_LENGTH(REPLACE(Path, ';', '')) AS pathLength
FROM `cosmic-octane-88917.analytics_us._VW_ApplicationExitEvent`
),

top_10 AS (
SELECT *,
      CONCAT(ROW_NUMBER() OVER(), '-Path_Freq_Rank') AS nickname
FROM (
      SELECT
        Path,
        COUNT(TimeStamp) count
      FROM `cosmic-octane-88917.analytics_us._VW_ApplicationExitEvent`
      WHERE FlowName in {0}
      AND EXTRACT(DATE FROM TimeStamp) BETWEEN '2020-06-01' AND '2020-10-31'
      GROUP BY Path
      ORDER BY count DESC
      LIMIT 10
      )
),

calc_values AS (
SELECT
      EXTRACT(DATE FROM TimeStamp) AS date,
      COUNT(DISTINCT(SessionId)) AS count,
      AVG(Duration) AS avg_duration,
      nickname,
      ANY_VALUE(Path) AS Path
FROM count_steps c
INNER JOIN
top_10 t
USING(Path)
GROUP BY date, nickname

)

SELECT *,
      AVG(count) OVER(PARTITION BY nickname ORDER BY date
                 ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS avg_14_day_count,
      AVG(avg_duration) OVER(PARTITION BY nickname ORDER BY date
                 ROWS BETWEEN 14 PRECEDING AND CURRENT ROW) AS avg_14_day_avg_duration
FROM calc_values