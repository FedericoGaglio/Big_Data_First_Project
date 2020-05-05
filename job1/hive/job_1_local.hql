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

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/job1_test_price.csv'
OVERWRITE INTO TABLE historical_stock_prices;

CREATE TABLE IF NOT EXISTS ticker_min_max_avg AS
SELECT ticker, min(close) AS min_price, max(close) AS max_price, avg(volume) AS avg_volume
FROM historical_stock_prices
WHERE YEAR(data)>=2008 AND YEAR(data)<=2018
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS ticker_first_last_data AS
SELECT ticker, min(TO_DATE(data)) AS first_data, max(TO_DATE(data)) AS last_data
FROM historical_stock_prices
WHERE YEAR(data)>=2008 AND YEAR(data)<=2018
GROUP BY ticker;

CREATE TABLE IF NOT EXISTS ticker_close_first_data AS
SELECT h.ticker, h.data, h.close AS first_price
FROM ticker_first_last_data AS t
JOIN historical_stock_prices AS h
ON h.ticker=t.ticker AND h.data=t.first_data;

CREATE TABLE IF NOT EXISTS ticker_close_last_data AS
SELECT h.ticker, h.data, h.close AS last_price
FROM ticker_first_last_data AS t
JOIN historical_stock_prices AS h
ON h.ticker=t.ticker AND h.data=t.last_data;

CREATE TABLE IF NOT EXISTS ticker_percentage AS
SELECT fc.ticker, (((lc.last_price-fc.first_price)/fc.first_price) * 100) AS inc_per
FROM ticker_close_last_data AS lc
JOIN ticker_close_first_data AS fc
ON lc.ticker=fc.ticker;

CREATE TABLE IF NOT EXISTS result_job_1 AS 
SELECT t.ticker, p.inc_per, t.min_price, t.max_price, t.avg_volume
FROM ticker_min_max_avg AS t
JOIN ticker_percentage AS p
ON t.ticker=p.ticker 
ORDER BY p.inc_per DESC limit 10;