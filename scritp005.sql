//*------------In-class lab02 Hive Query---------------------*/


CREATE EXTERNAL TABLE IF NOT EXISTS stocks (
ymd STRING,
symbol STRING,
price_open FLOAT,
price_high FLOAT,
price_low FLOAT,
price_close FLOAT,
price_adj_close FLOAT,
volume INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION 'bigdata/hive/'
TBLPROPERTIES('skip.header.line.count'='1');


LOAD DATA INPATH '/opt/hive/bigdata/hive/stocks_data_copy_1.csv' INTO TABLE stocks;



SELECT symbol, AVG(volume) AS avg_daily_volume
FROM stocks
GROUP BY symbol
HAVING AVG(volume) > 1000000;


SELECT symbol, AVG(volume) AS avg_volume_2020
FROM stocks
WHERE year(ymd) = 2020
GROUP BY symbol
ORDER BY avg_volume_2020 DESC
LIMIT 3;


SELECT symbol, SUM(volume) AS total_volume
FROM stocks
WHERE symbol LIKE 'C%'
GROUP BY symbol
ORDER BY total_volume DESC
LIMIT 3;

SELECT DISTINCT symbol
FROM stocks
WHERE price_close > 41;

SELECT symbol, 
       ROUND((price_high - price_low), 2) AS intraday_change
FROM stocks
ORDER BY intraday_change DESC
LIMIT 10;