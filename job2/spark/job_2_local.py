#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setMaster("local[*]").setAppName("Job2")
sc = SparkContext(conf=conf)

def first_close_date(x, y):
    if x[0] > y[0]:
        return y
    else:
        return x


def last_close_date(x, y):
    if x[0] > y[0]:
        return x
    else:
        return y

def parse_year(date):
    return date[0:4]

input_file = sc.textFile(
    "file:///Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/job2_test_price.csv") \
    .map(lambda line: line.split(","))

input_file_2 = sc.textFile(
    "file:///Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/job2_test.csv") \
    .map(lambda line: line.split(","))

input_file = input_file \
    .filter(lambda line: line[0] != "ticker") \
    .filter(lambda line: "2008" <= line[7][0:4] <= "2018")

input_file_2 = input_file_2 \
    .filter(lambda line: line[0] != "ticker") \
    .filter(lambda line: line[3] != "N/A") \

# (ticker, ((close, volume, date), sector))
total = input_file \
    .map(lambda line: (line[0], (line[2], line[6], line[7]))) \
    .join(input_file_2.map(lambda line: (line[0], (line[3]))))

# (ticker, close, volume, date, sector)
total = total \
    .map(lambda line: (line[0], float(line[1][0][0]), float(line[1][0][1]), line[1][0][2], line[1][1]))

# ((sector, year), avg_volume)
avg_volume = total \
    .map(lambda line: ((line[4], parse_year(line[3])), (line[2], 1))) \
    .reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1])) \
    .map(lambda line: ((line[0][0], line[0][1]), line[1][0]/line[1][1]))

# ((sector, ticker, year), (first_date_close, close)))
first_data = total \
    .map(lambda line: ((line[4], line[0], parse_year(line[3])), (line[3], line[1]))) \
    .reduceByKey(lambda x, y: first_close_date(x, y))

# ((sector, ticker, year), (last_date_close, close)))
last_data = total \
    .map(lambda line: ((line[4], line[0], parse_year(line[3])), (line[3], line[1]))) \
    .reduceByKey(lambda x, y: last_close_date(x, y))

# ((sector, ticker, year), ((first_date_close, close), (last_date_close, close)))
year_change = first_data \
    .join(last_data)

# ((sector, ticker, year), var)
year_change = year_change \
    .map(lambda line: (line[0], (((line[1][1][1]-line[1][0][1])/line[1][0][1]) * 100) ))

# ((sector, year), avg_var)
year_change = year_change \
    .map(lambda line: ((line[0][0], line[0][2]), (line[1], 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda line: ((line[0][0], line[0][1]), line[1][0] / line[1][1]))

# ((sector, ticker, year), avg_close)
daily_price = total \
    .map(lambda line: ((line[4], line[0], parse_year(line[3])), (line[1], 1))) \
    .reduceByKey(lambda x, y: (x[0]+y[0],x[1]+y[1])) \
    .map(lambda line: (line[0], line[1][0]/line[1][1]))

# ((sector, year), avg_avg_close)
daily_price = daily_price \
    .map(lambda line: ((line[0][0], line[0][2]), (line[1], 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda line: ((line[0][0], line[0][1]), line[1][0] / line[1][1]))

# ((sector, year), ((avg_volume, year_change), avg_avg_close))
result = avg_volume \
    .join(year_change) \
    .join(daily_price)

# (sector, year, avg_volume, year_change, avg_avg_close)
result = result \
    .map(lambda line: (line[0][0], line[0][1], line[1][0][0],line[1][0][1], line[1][1]))

result.coalesce(1).saveAsTextFile("file:///Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job2/spark/result/job_2_result")




