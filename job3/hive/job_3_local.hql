CREATE TABLE IF NOT EXISTS historical_stock_prices (
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

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/job3_test_price.csv' 
OVERWRITE INTO TABLE historical_stock_prices;

CREATE TABLE IF NOT EXISTS historical_stock (
	ticker STRING, 
	exchanges STRING, 
	name STRING, 
	sector STRING, 
	industry STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
TBLPROPERTIES("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/job3_test.csv' 
OVERWRITE INTO TABLE historical_stock;

CREATE TABLE IF NOT EXISTS main_table AS 
SELECT hs.name, hsp.ticker, hsp.data, hsp.close
FROM historical_stock AS hs 
JOIN historical_stock_prices AS hsp 
ON hsp.ticker=hs.ticker 
WHERE YEAR(hsp.data)>=2016 AND YEAR(hsp.data)<=2018 
AND hs.name!='N/A';

CREATE TABLE IF NOT EXISTS ticker_first_last_data_year AS 
SELECT name, ticker, min(TO_DATE(data)) AS first_data, max(TO_DATE(data)) AS last_data 
FROM main_table 
GROUP BY name, ticker, YEAR(data);

CREATE TABLE IF NOT EXISTS ticker_close_first_data AS 
SELECT t.name, h.ticker, h.data, h.close AS first_price
FROM ticker_first_last_data_year AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.first_data;

CREATE TABLE IF NOT EXISTS ticker_close_last_data AS 
SELECT t.name, h.ticker, h.data, h.close AS last_price
FROM ticker_first_last_data_year AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.last_data;

CREATE TABLE IF NOT EXISTS ticker_percentage AS 
SELECT fc.name, fc.ticker, YEAR(fc.data) as anno, (((lc.last_price-fc.first_price)/fc.first_price) * 100) AS inc_per 
FROM ticker_close_last_data AS lc
JOIN ticker_close_first_data AS fc
ON lc.ticker=fc.ticker AND YEAR(lc.data)=YEAR(fc.data);

CREATE TABLE IF NOT EXISTS first_year AS 
SELECT name, inc_per
FROM ticker_percentage
WHERE anno == 2016
ORDER BY name;

CREATE TABLE IF NOT EXISTS second_year AS 
SELECT name, inc_per
FROM ticker_percentage
WHERE anno == 2017
ORDER BY name;

CREATE TABLE IF NOT EXISTS third_year AS 
SELECT name, inc_per
FROM ticker_percentage
WHERE anno == 2018
ORDER BY name;

CREATE TABLE IF NOT EXISTS result_job_3 AS
SELECT first_year.name, first_year.inc_per AS primo, second_year.inc_per AS secondo, third_year.inc_per AS terzo
FROM first_year, second_year, third_year
WHERE first_year.name = second_year.name AND second_year.name = third_year.name
ORDER BY primo, secondo, terzo;
