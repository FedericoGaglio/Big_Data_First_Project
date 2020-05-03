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

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/dataset/job2_test_price.csv' 
OVERWRITE INTO TABLE historical_stock_prices;

CREATE TABLE historical_stock (
	ticker STRING, 
	exchanges STRING, 
	name STRING, 
	sector STRING, 
	industry STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/dataset/job2_test.csv' 
OVERWRITE INTO TABLE historical_stock;

CREATE TABLE IF NOT EXISTS main_table AS 
SELECT hs.sector, hsp.ticker, hsp.data, hsp.close, hsp.volume 
FROM historical_stock AS hs 
JOIN historical_stock_prices AS hsp 
ON hsp.ticker=hs.ticker 
WHERE YEAR(hsp.data)>=2008 AND YEAR(hsp.data)<=2018 
AND hs.sector!='N/A';

CREATE TABLE IF NOT EXISTS ticker_data_volume AS 
SELECT sector, ticker, YEAR(data) AS anno, SUM(volume) AS sum_volume 
FROM main_table 
GROUP BY sector, ticker, YEAR(data);

CREATE TABLE IF NOT EXISTS sector_data_avg_volume AS 
SELECT sector, anno, AVG(sum_volume) AS avg_volume 
FROM ticker_data_volume 
GROUP BY sector, anno;

CREATE TABLE IF NOT EXISTS ticker_first_last_data_year AS 
SELECT sector, ticker, min(TO_DATE(data)) AS first_data, max(TO_DATE(data)) AS last_data 
FROM main_table 
GROUP BY sector, ticker, YEAR(data);

CREATE TABLE IF NOT EXISTS ticker_close_first_data AS 
SELECT t.sector, h.ticker, h.data, h.close AS first_price
FROM ticker_first_last_data_year AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.first_data;

CREATE TABLE IF NOT EXISTS ticker_close_last_data AS 
SELECT t.sector, h.ticker, h.data, h.close AS last_price
FROM ticker_first_last_data_year AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.last_data;

CREATE TABLE IF NOT EXISTS ticker_percentage AS 
SELECT fc.sector, fc.ticker, YEAR(fc.data) as anno, (((lc.last_price-fc.first_price)/fc.first_price) * 100) AS inc_per 
FROM ticker_close_last_data AS lc
JOIN ticker_close_first_data AS fc
ON lc.ticker=fc.ticker AND YEAR(lc.data)=YEAR(fc.data);

CREATE TABLE IF NOT EXISTS sector_avg_percentage AS 
SELECT sector, anno, AVG(inc_per) AS avg_per
FROM ticker_percentage
GROUP BY sector, anno;

CREATE TABLE IF NOT EXISTS ticker_avg_price AS
SELECT sector, ticker, YEAR(data) AS anno, AVG(close) AS avg_ticker_price
FROM main_table
GROUP BY sector, ticker, YEAR(data);

CREATE TABLE IF NOT EXISTS sector_avg_price AS
SELECT sector, anno, AVG(avg_ticker_price) AS avg_sector_price
FROM ticker_avg_price
GROUP BY sector, anno;

CREATE TABLE IF NOT EXISTS result_job_2 AS 
SELECT a.sector, a.anno, a.avg_volume, b.avg_per, c.avg_sector_price 
FROM sector_data_avg_volume AS a
JOIN sector_avg_percentage AS b
JOIN sector_avg_price AS c 
ON a.sector=b.sector AND b.sector=c.sector AND a.anno=b.anno AND b.anno=c.anno
ORDER BY sector, anno;