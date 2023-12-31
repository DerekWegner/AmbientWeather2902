#! /usr/bin/env bash

# Get local copy of weather Sqlite DB:

rsync -v rp400:repos/Weather/var/weather.sqlite /tmp

sqlite3 /tmp/weather.sqlite <<EOF
SELECT count(*) FROM weather;


DROP TABLE IF EXISTS weather_partition;

CREATE TEMPORARY TABLE weather_partition
(
	dateutc_start STRING,
	dateutc_end STRING,
	gap INT
	,primary key (dateutc_start, dateutc_end)
	);
	
WITH weather_gap1 AS
(
SELECT dateutc, 
IFNULL(strftime('%s', dateutc) - strftime('%s', lag (dateutc) OVER (ORDER BY dateutc)), 0) AS gap
FROM weather
),
datasets AS 
(
	SELECT 
		dateutc AS dateutc_start,
		gap
		FROM weather_gap1 
		WHERE gap >= 5*60 OR gap = 0
	),
datasets1 AS (
	SELECT dateutc_start,
	IFNULL((SELECT MIN(dateutc_start) FROM datasets XXX WHERE XXX.dateutc_start > datasets.dateutc_start), '2050-01-01 00:00:00') AS dateutc_end,
	gap
	from datasets
	)
INSERT INTO weather_partition
	SELECT dateutc_start, dateutc_end, gap
	FROM datasets1
	ORDER BY dateutc_start
	;

DROP TABLE IF EXISTS weather_runs;
CREATE TABLE weather_runs
(
	dateutc STRING primary key,
	row_number int,
	row_number_rain int,
	row_number_wind int
	);
INSERT INTO weather_runs
SELECT dateutc, 
	row_number() OVER (ORDER BY dateutc) AS row_number,
	row_number() OVER (ORDER BY dateutc) - row_number() OVER (PARTITION BY dateutc_start,hourlyrainin>0 ORDER BY dateutc) 
		AS row_number_rain,
	row_number() OVER (ORDER BY dateutc) - row_number() OVER (PARTITION BY dateutc_start,windspeedmph>0 ORDER BY dateutc) 
		AS row_number_wind
FROM weather
JOIN weather_partition ON (weather.dateutc >= weather_partition.dateutc_start AND weather.dateutc < weather_partition.dateutc_end)
ORDER BY dateutc
;

SELECT 'Winding';

SELECT 
DATETIME(MIN(dateutc), 'localtime') AS 'started PT', 
DATETIME(MAX(dateutc), 'localtime') AS 'ended PT',
ROUND(AVG(windspeedmph), 1) AS avg,
MAX(windspeedmph) AS max,
STRFTIME('%H:%M:%S', strftime('%s', MAX(dateutc)) - strftime('%s', MIN(dateutc)), 'unixepoch') AS duration
FROM weather
JOIN weather_partition ON (weather.dateutc >= weather_partition.dateutc_start AND weather.dateutc < weather_partition.dateutc_end)
JOIN weather_runs USING (dateutc)
WHERE windspeedmph > 0
GROUP BY row_number_wind
HAVING duration >= '00:30:00'
ORDER BY duration DESC, weather.dateutc
LIMIT 5
;

SELECT 'Raining';

SELECT 
DATETIME(MIN(dateutc), 'localtime') AS 'started PT', 
DATETIME(MAX(dateutc), 'localtime') AS 'ended PT',
ROUND(AVG(hourlyrainin), 1) AS avg,
ROUND(MAX(hourlyrainin), 4) AS max,
STRFTIME('%H:%M:%S', strftime('%s', MAX(dateutc)) - strftime('%s', MIN(dateutc)), 'unixepoch') AS duration
FROM weather
JOIN weather_partition ON (weather.dateutc >= weather_partition.dateutc_start AND weather.dateutc < weather_partition.dateutc_end)
JOIN weather_runs USING (dateutc)
WHERE hourlyrainin > 0
GROUP BY row_number_rain
HAVING duration >= '00:10:00'
ORDER BY duration DESC, weather.dateutc
LIMIT 5
;

DROP TABLE IF EXISTS days;
CREATE TEMPORARY TABLE days(
	day STRING PRIMARY KEY,
	month STRING);

INSERT INTO days
  WITH RECURSIVE
    cte(x) AS (
       SELECT DATE((SELECT MIN(weather.dateutc) FROM weather WHERE dateutc >= 2023))
       UNION ALL
       SELECT DATE(x, "1 day")
         FROM cte
		 WHERE x <= (SELECT MAX(weather.dateutc) FROM weather WHERE dateutc >= 2023)
        -- LIMIT 1000
  )
SELECT x, substr(x, 6, 2) FROM cte;

.mode column

CREATE TEMPORARY TABLE daily_rainfall AS
SELECT month, day, IFNULL(MAX(dailyrainin), 0.0) AS dailyrainin
FROM days
LEFT OUTER JOIN weather ON (DATE(weather.dateutc, 'localtime') = days.day)
GROUP BY day
ORDER BY dailyrainin DESC;

-- Rainy/dry days by month:

SELECT month, SUM(dailyrainin>0) AS raining, SUM(dailyrainin=0) AS dry, COUNT(*)
FROM daily_rainfall
GROUP BY month
ORDER BY month
;

-- Rainest days

SELECT day, dailyrainin
FROM daily_rainfall
ORDER BY dailyrainin DESC
LIMIT 5;


EOF


exit




DROP TABLE IF EXISTS hourly_weather;
CREATE TEMPORARY TABLE hourly_weather(
	hour STRING PRIMARY KEY,
	month STRING,
	count INTEGER,
	hourlyrainin REAL,
	hourlywindspeedmph REAL,
	hourlywindgustmph REAL,
	hourlymintempf,
	hourlymaxtempf);

INSERT INTO hourly_weather
  WITH RECURSIVE
    cte(x) AS (
		SELECT DATETIME(DATE((SELECT MIN(weather.dateutc) FROM weather WHERE dateutc >= 2023)))
       UNION ALL
       SELECT DATETIME(x, "1 hour")
         FROM cte
		 WHERE x <= (SELECT MAX(weather.dateutc) FROM weather WHERE dateutc >= 2023)
        -- LIMIT 1000
  )
SELECT x, 
substr(x, 6, 2) AS month, 
SUM(dailyrainin IS NOT NULL),
ROUND(IFNULL(MAX(weather.dailyrainin) - MIN(weather.dailyrainin), 0.0), 3) AS rain_hour,
ROUND(IFNULL(AVG(weather.windspeedmph), 0.0), 3) AS hourlywindspeedmph,
IFNULL(MAX(weather.windgustmph), 0.0) AS hourlywindgustmph,
MIN(weather.tempf) AS hourlymintempf,
MAX(weather.tempf) AS hourlymaxtempf
FROM cte
LEFT OUTER JOIN weather ON ((weather.dateutc) >= x AND (weather.dateutc) < DATETIME(x, "1 HOUR"))
GROUP BY x
;

SELECT DATETIME(hour, 'localtime'), * FROM hourly_weather ORDER BY hour;

SELECT 'max wind gust' AS type, * FROM hourly_weather WHERE hourlywindgustmph = (SELECT MAX(hourlywindgustmph) FROM hourly_weather)
UNION ALL
SELECT 'max wind speed', * FROM hourly_weather WHERE hourlywindspeedmph = (SELECT MAX(hourlywindspeedmph) FROM hourly_weather)
UNION ALL
SELECT 'rain', * FROM hourly_weather WHERE hourlyrainin = (SELECT MAX(hourlyrainin) FROM hourly_weather)
UNION ALL
SELECT 'min temp', * FROM hourly_weather WHERE hourlymintempf = (SELECT MIN(hourlymintempf) FROM hourly_weather)
UNION ALL
SELECT 'max temp', * FROM hourly_weather WHERE hourlymaxtempf = (SELECT MAX(hourlymaxtempf) FROM hourly_weather)
ORDER BY hour
;


DROP TABLE IF EXISTS hourly_weather;
CREATE TEMPORARY TABLE hourly_weather(
	hour STRING PRIMARY KEY,
	month STRING,
	count INTEGER,
	hourlyrainin REAL,
	hourlywindspeedmph REAL,
	hourlywindgustmph REAL,
	hourlymintempf,
	hourlymaxtempf);

INSERT INTO hourly_weather
  WITH RECURSIVE
    cte(x) AS (
		SELECT DATETIME(DATE((SELECT MIN(weather.dateutc) FROM weather WHERE dateutc >= 2023)))
       UNION ALL
       SELECT DATETIME(x, "1 hour")
         FROM cte
		 WHERE x <= (SELECT MAX(weather.dateutc) FROM weather WHERE dateutc >= 2023)
        -- LIMIT 1000
  )
SELECT x, 
substr(x, 6, 2) AS month, 
SUM(dailyrainin IS NOT NULL),
ROUND(IFNULL(MAX(weather.dailyrainin) - MIN(weather.dailyrainin), 0.0), 3) AS rain_hour,
ROUND(IFNULL(AVG(weather.windspeedmph), 0.0), 3) AS hourlywindspeedmph,
IFNULL(MAX(weather.windgustmph), 0.0) AS hourlywindgustmph,
MIN(weather.tempf) AS hourlymintempf,
MAX(weather.tempf) AS hourlymaxtempf
FROM cte
LEFT OUTER JOIN weather ON ((weather.dateutc) >= x AND (weather.dateutc) < DATETIME(x, "1 HOUR"))
GROUP BY x
;

SELECT DATETIME(hour, 'localtime'), * FROM hourly_weather ORDER BY hour;

SELECT 'max wind gust' AS type, * FROM hourly_weather WHERE hourlywindgustmph = (SELECT MAX(hourlywindgustmph) FROM hourly_weather)
UNION ALL
SELECT 'max wind speed', * FROM hourly_weather WHERE hourlywindspeedmph = (SELECT MAX(hourlywindspeedmph) FROM hourly_weather)
UNION ALL
SELECT 'rain', * FROM hourly_weather WHERE hourlyrainin = (SELECT MAX(hourlyrainin) FROM hourly_weather)
UNION ALL
SELECT 'min temp', * FROM hourly_weather WHERE hourlymintempf = (SELECT MIN(hourlymintempf) FROM hourly_weather)
UNION ALL
SELECT 'max temp', * FROM hourly_weather WHERE hourlymaxtempf = (SELECT MAX(hourlymaxtempf) FROM hourly_weather)
ORDER BY hour
;

WITH xxx AS
(SELECT hour, hourlyrainin,
row_number() OVER (ORDER BY hour) - row_number() OVER (PARTITION BY hourlyrainin>0 ORDER BY hour) AS row_number_rainin_partition
FROM hourly_weather
)
SELECT MIN(hour), MAX(hour), MIN(hourlyrainin), COUNT(*) AS hours, row_number_rainin_partition
FROM xxx
GROUP BY row_number_rainin_partition
HAVING MIN(hourlyrainin)>0
ORDER BY hours DESC, hour;


DROP TABLE IF EXISTS hourly_weather;
CREATE TEMPORARY TABLE hourly_weather(
	hour STRING PRIMARY KEY,
	month STRING,
	count INTEGER,
	hourlyrainin REAL,
	hourlywindspeedmph REAL,
	hourlywindgustmph REAL,
	hourlymintempf,
	hourlymaxtempf);

INSERT INTO hourly_weather
  WITH RECURSIVE
    cte(x) AS (
		SELECT DATETIME(DATE((SELECT MIN(weather.dateutc) FROM weather WHERE dateutc >= 2023)))
       UNION ALL
       SELECT DATETIME(x, "1 hour")
         FROM cte
		 WHERE x <= (SELECT MAX(weather.dateutc) FROM weather WHERE dateutc >= 2023)
        -- LIMIT 1000
  )
SELECT x, 
substr(x, 6, 2) AS month, 
SUM(dailyrainin IS NOT NULL),
ROUND(IFNULL(MAX(weather.dailyrainin) - MIN(weather.dailyrainin), 0.0), 3) AS rain_hour,
ROUND(IFNULL(AVG(weather.windspeedmph), 0.0), 3) AS hourlywindspeedmph,
IFNULL(MAX(weather.windgustmph), 0.0) AS hourlywindgustmph,
MIN(weather.tempf) AS hourlymintempf,
MAX(weather.tempf) AS hourlymaxtempf
FROM cte
LEFT OUTER JOIN weather ON ((weather.dateutc) >= x AND (weather.dateutc) < DATETIME(x, "1 HOUR")  
	AND tempf NOT NULL AND windspeedmph NOT NULL AND windgustmph NOT NULL)
GROUP BY x
;

-- SELECT DATETIME(hour, 'localtime'), * FROM hourly_weather ORDER BY hour;

SELECT 'max wind gust' AS type, * FROM hourly_weather WHERE hourlywindgustmph = (SELECT MAX(hourlywindgustmph) FROM hourly_weather)
UNION ALL
SELECT 'max wind speed', * FROM hourly_weather WHERE hourlywindspeedmph = (SELECT MAX(hourlywindspeedmph) FROM hourly_weather)
UNION ALL
SELECT 'rain', * FROM hourly_weather WHERE hourlyrainin = (SELECT MAX(hourlyrainin) FROM hourly_weather)
UNION ALL
SELECT 'min temp', * FROM hourly_weather WHERE hourlymintempf = (SELECT MIN(hourlymintempf) FROM hourly_weather)
UNION ALL
SELECT 'max temp', * FROM hourly_weather WHERE hourlymaxtempf = (SELECT MAX(hourlymaxtempf) FROM hourly_weather)
ORDER BY hour
;

WITH xxx AS
(SELECT hour, hourlyrainin,
row_number() OVER (ORDER BY hour) - row_number() OVER (PARTITION BY hourlyrainin>0 ORDER BY hour) AS row_number_rainin_partition
FROM hourly_weather
)
SELECT MIN(hour), MAX(hour), MIN(hourlyrainin), COUNT(*) AS hours, row_number_rainin_partition
FROM xxx
GROUP BY row_number_rainin_partition
HAVING MIN(hourlyrainin)>0
ORDER BY hours DESC, hour
LIMIT 5
;


WITH zzz AS
(SELECT *, 
-- 'Wind speed >= 5' as msg, hourlywindspeedmph>=5.0 AS burfl,
-- 'tempf <= 45' as msg, hourlymintempf<=45.0 AS burfl,
'tempf >= 50' as msg, hourlymaxtempf>=50.0 AS burfl,
-- 'windy' as msg, houlywindspeedmph>0 AS burfl,
hourlymintempf, hourlymaxtempf
FROM hourly_weather),
xxx AS
(SELECT hour, hourlywindspeedmph, hourlymintempf, hourlymaxtempf, msg, burfl,
row_number() OVER (ORDER BY hour) - row_number() OVER (PARTITION BY burfl ORDER BY hour) AS row_number_partition
FROM zzz
)
SELECT msg, MIN(hour), MAX(hour), COUNT(*) AS hours, MIN(hourlywindspeedmph), hourlymintempf, hourlymaxtempf
FROM xxx
GROUP BY row_number_partition
HAVING burfl
ORDER BY hours DESC, hour
LIMIT 5
;




