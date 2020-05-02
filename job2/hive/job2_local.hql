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

CREATE TABLE IF NOT EXISTS first_pricipal_table AS 
SELECT hs.sector, hsp.ticker, hsp.data, hsp.close, hsp.volume 
FROM historical_stock AS hs 
JOIN historical_stock_prices AS hsp 
ON hsp.ticker=hs.ticker 
WHERE YEAR(hsp.data)>=2008 AND YEAR(hsp.data)<=2018 
AND hs.sector!='N/A';

CREATE TABLE IF NOT EXISTS ticker_data_volume AS 
SELECT sector, ticker, YEAR(data) AS anno, SUM(volume) AS sum_volume 
FROM first_pricipal_table 
GROUP BY sector, ticker, YEAR(data);

--sector_data_volume
CREATE TABLE IF NOT EXISTS sector_data_volume AS 
SELECT sector, anno, AVG(sum_volume) AS avg_volume 
FROM ticker_data_volume 
GROUP BY sector, anno;


CREATE TABLE IF NOT EXISTS sector_data_min_max AS 
SELECT sector, ticker, min(TO_DATE(data)) AS min_data, max(TO_DATE(data)) AS max_data 
FROM first_pricipal_table 
GROUP BY sector, ticker, YEAR(data);

CREATE TABLE IF NOT EXISTS ticker_close_min_data AS 
SELECT t.sector, h.ticker, h.data, h.close AS min_price
FROM sector_data_min_max AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.min_data;

CREATE TABLE IF NOT EXISTS ticker_close_max_data AS 
SELECT t.sector, h.ticker, h.data, h.close AS max_price
FROM sector_data_min_max AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.max_data;


CREATE TABLE IF NOT EXISTS ticker_percentuale AS 
SELECT mi.sector, mi.ticker, YEAR(mi.data) as anno, (((ma.max_price-mi.min_price)/mi.min_price) * 100) AS inc_perc 
FROM ticker_close_max_data AS ma
JOIN ticker_close_min_data AS mi
ON ma.ticker=mi.ticker AND YEAR(ma.data)=YEAR(mi.data);

--sector_avg_percentuale
CREATE TABLE IF NOT EXISTS sector_avg_percentuale AS 
SELECT sector, anno, AVG(inc_perc) AS avg_perc
FROM ticker_percentuale
GROUP BY sector, anno;

CREATE TABLE IF NOT EXISTS ticker_avg_price AS
SELECT sector, ticker, YEAR(data) AS anno, AVG(close) AS avg_ticker_price
FROM first_pricipal_table
GROUP BY sector, ticker, YEAR(data);

--sector_avg_price
CREATE TABLE IF NOT EXISTS sector_avg_price AS
SELECT sector, anno, AVG(avg_ticker_price) AS avg_sector_price
FROM ticker_avg_price
GROUP BY sector, anno;


CREATE TABLE IF NOT EXISTS result_job_2 AS 
SELECT a.sector, a.anno, a.avg_volume, b.avg_perc, c.avg_sector_price 
FROM sector_data_volume AS a
JOIN sector_avg_percentuale AS b
JOIN sector_avg_price AS c 
ON a.sector=b.sector AND b.sector=c.sector AND a.anno=b.anno AND b.anno=c.anno
ORDER BY sector, anno;