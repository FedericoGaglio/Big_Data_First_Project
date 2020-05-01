DROP TABLE IF EXISTS historical_stock_prices;
DROP TABLE IF EXISTS ticker_close_max_data;
DROP TABLE IF EXISTS ticker_close_min_data;
DROP TABLE IF EXISTS ticker_max_data;
DROP TABLE IF EXISTS ticker_min_data;
DROP TABLE IF EXISTS ticker_min_max_avg;
DROP TABLE IF EXISTS ticker_percentuale;
DROP TABLE IF EXISTS result_job_1;


CREATE TABLE historical_stock_prices (
	ticker STRING, 
	open STRING, 
	close double, 
	adj_close STRING, 
	low STRING, 
	high STRING, 
	volume int, 
	data STRING)

ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/dataset/job1_test_price.csv' 
OVERWRITE INTO TABLE historical_stock_prices;

CREATE VIEW IF NOT EXISTS ticker_min_max_avg AS 
SELECT ticker, min(close) AS min_price, max(close) AS max_price, avg(volume) AS avg_volume 
FROM historical_stock_prices 
WHERE YEAR(data)>=2008 AND YEAR(data)<=2018 GROUP BY ticker;

CREATE VIEW IF NOT EXISTS ticker_min_data AS 
SELECT ticker, min(TO_DATE(data)) AS min_data 
FROM historical_stock_prices 
WHERE YEAR(data)==2008 
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS ticker_max_data AS 
SELECT ticker, max(TO_DATE(data)) AS max_data 
FROM historical_stock_prices 
WHERE YEAR(data)==2018 
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS ticker_close_min_data AS 
SELECT h.ticker, h.data, h.close 
FROM ticker_min_data AS t, historical_stock_prices AS h 
WHERE h.ticker=t.ticker AND h.data=t.min_data;

CREATE TABLE IF NOT EXISTS ticker_close_max_data AS 
SELECT h.ticker, h.data, h.close 
FROM ticker_max_data AS t, historical_stock_prices AS h 
WHERE h.ticker=t.ticker AND h.data=t.max_data;

CREATE TABLE IF NOT EXISTS ticker_percentuale AS 
SELECT mi.ticker, ((ma.close-mi.close)/mi.close) AS inc_perc 
FROM ticker_close_max_data AS ma join ticker_close_min_data AS mi on ma.ticker=mi.ticker;

CREATE TABLE IF NOT EXISTS result_job_1 AS 
SELECT a.ticker, b.inc_perc, a.min_price, a.max_price, a.avg_volume 
FROM ticker_min_max_avg AS a join ticker_percentuale AS b on a.ticker=b.ticker 
ORDER BY b.inc_perc DESC limit 10;