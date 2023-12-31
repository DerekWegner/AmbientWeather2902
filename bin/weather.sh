#! /usr/bin/env bash

# Called via
#
#    weather.sh [port] [sqlite3 file]

# stationtype=AMBWeatherV4.3.4&PASSKEY=84:F3:EB:25:1C:30&dateutc=2023-01-25+03:25:40&tempinf=66.2&humidityin=45&baromrelin=30.739&baromabsin=30.535&tempf=44.1&humidity=97&winddir=319&windspeedmph=0.0&windgustmph=0.0&maxdailygust=10.3&hourlyrainin=0.000&eventrainin=0.000&dailyrainin=0.000&weeklyrainin=0.000&monthlyrainin=2.091&totalrainin=160.228&solarradiation=0.00&uv=0&batt_co2=1

while true
do
    
# Wait for HTTP request, parse it via ugly string manipulations, and set Bash variables for each parameter:

    for v in $(nc -l -p ${1:-8080} | sed -u 's/&/ /g' | egrep -o 'stationtype.* '); do export $v; done;

    if [ ${e} > 1 ]; then
	echo "Bad exit code from nc and friends: ${e}"
	logger -t "weather" -p user.notice "Bad exit code from nc and friends: ${e}"
    else
	echo Received POST: $dateutc	

# Import the record into a Sqlite3 DB:

sqlite3 ${2:-weather.sqlite3} <<EOF

-- DROP TABLE IF EXISTS weather;

CREATE TABLE IF NOT EXISTS weather
       (
       stationtype text,
       dateutc text PRIMARY KEY,
       tempinf real,
       humidityin INT,
       baromrelin real,
       baromabsin REAL,
       tempf real,
       humidity INT,
       winddir INT,
       windspeedmph REAL,
       windgustmph REAL,
       maxdailygust REAL,
       hourlyrainin REAL,
       eventrainin REAL,
       dailyrainin REAL,
       weeklyrainin REAL,
       monthlyrainin REAL,
       totalrainin REAL,
       solarradiation REAL,
       uv INT,
       batt_co2 INT
       );

INSERT OR REPLACE INTO weather 
VALUES (
       "$stationtype",
       "${dateutc/+/ }",
       $tempinf,
       $humidityin,
       $baromrelin,
       $baromabsin,
       $tempf,
       $humidity,
       $winddir,
       $windspeedmph,
       $windgustmph,
       $maxdailygust,
       $hourlyrainin,
       $eventrainin,
       $dailyrainin,
       $weeklyrainin,
       $monthlyrainin,
       $totalrainin,
       $solarradiation,
       $uv,
       $batt_co2
);

EOF
    fi
    
done
