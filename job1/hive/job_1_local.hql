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

CREATE TABLE IF NOT EXISTS ticker_min_max_avg AS 
SELECT ticker, min(close) AS min_price, max(close) AS max_price, avg(volume) AS avg_volume 
FROM historical_stock_prices 
WHERE YEAR(data)>=2008 AND YEAR(data)<=2018 
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS ticker_min_max_data AS 
SELECT ticker, min(TO_DATE(data)) AS min_data, max(TO_DATE(data)) AS max_data
FROM historical_stock_prices 
WHERE YEAR(data)>=2008 AND YEAR(data)<=2018 
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS ticker_close_min_data AS 
SELECT h.ticker, h.data, h.close AS min_price
FROM ticker_min_max_data AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.min_data;

CREATE TABLE IF NOT EXISTS ticker_close_max_data AS 
SELECT h.ticker, h.data, h.close AS max_price
FROM ticker_min_max_data AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.max_data;

CREATE TABLE IF NOT EXISTS ticker_percentuale AS 
SELECT mi.ticker, (((ma.max_price-mi.min_price)/mi.min_price) * 100) AS inc_perc 
FROM ticker_close_max_data AS ma
JOIN ticker_close_min_data AS mi
ON ma.ticker=mi.ticker;

CREATE TABLE IF NOT EXISTS result_job_1 AS 
SELECT t.ticker, p.inc_perc, t.min_price, t.max_price, t.avg_volume 
FROM ticker_min_max_avg AS t
JOIN ticker_percentuale AS p
ON t.ticker=p.ticker 
ORDER BY p.inc_perc DESC limit 10;