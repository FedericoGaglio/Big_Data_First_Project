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

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/dataset/job3_test_price.csv' 
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

LOAD DATA LOCAL INPATH '/Users/alessio/Documents/Università/big-data/Big_Data_First_Project/dataset/job3_test.csv' 
OVERWRITE INTO TABLE historical_stock;

CREATE TABLE IF NOT EXISTS first_pricipal_table AS 
SELECT hs.name, hsp.ticker, hsp.data, hsp.close
FROM historical_stock AS hs 
JOIN historical_stock_prices AS hsp 
ON hsp.ticker=hs.ticker 
WHERE YEAR(hsp.data)>=2016 AND YEAR(hsp.data)<=2018 
AND hs.name!='N/A';

CREATE TABLE IF NOT EXISTS sector_data_min_max AS 
SELECT name, ticker, min(TO_DATE(data)) AS min_data, max(TO_DATE(data)) AS max_data 
FROM first_pricipal_table 
GROUP BY name, ticker, YEAR(data);

CREATE TABLE IF NOT EXISTS ticker_close_min_data AS 
SELECT t.name, h.ticker, h.data, h.close AS min_price
FROM sector_data_min_max AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.min_data;

CREATE TABLE IF NOT EXISTS ticker_close_max_data AS 
SELECT t.name, h.ticker, h.data, h.close AS max_price
FROM sector_data_min_max AS t
JOIN historical_stock_prices AS h 
ON h.ticker=t.ticker AND h.data=t.max_data;


CREATE TABLE IF NOT EXISTS ticker_percentuale AS 
SELECT mi.name, mi.ticker, YEAR(mi.data) as anno, (((ma.max_price-mi.min_price)/mi.min_price) * 100) AS inc_perc 
FROM ticker_close_max_data AS ma
JOIN ticker_close_min_data AS mi
ON ma.ticker=mi.ticker AND YEAR(ma.data)=YEAR(mi.data);


CREATE TABLE IF NOT EXISTS first_year AS 
SELECT name, inc_perc
FROM ticker_percentuale
WHERE anno == 2016
ORDER BY name;

CREATE TABLE IF NOT EXISTS second_year AS 
SELECT name, inc_perc
FROM ticker_percentuale
WHERE anno == 2017
ORDER BY name;

CREATE TABLE IF NOT EXISTS third_year AS 
SELECT name, inc_perc
FROM ticker_percentuale
WHERE anno == 2018
ORDER BY name;

CREATE TABLE IF NOT EXISTS res AS
SELECT first_year.name, first_year.inc_perc AS primo, second_year.inc_perc AS secondo, third_year.inc_perc AS terzo
FROM first_year, second_year, third_year
WHERE first_year.name = second_year.name AND second_year.name = third_year.name
ORDER BY primo, secondo, terzo;
