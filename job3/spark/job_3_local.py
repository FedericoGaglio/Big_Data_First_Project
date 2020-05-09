#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setMaster("local[*]").setAppName("Job3")
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
    "file:///Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/job3_test_price.csv") \
    .map(lambda line: line.split(","))

input_file_2 = sc.textFile(
    "file:///Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/job3_test.csv") \
    .map(lambda line: line.split(","))

input_file = input_file \
    .filter(lambda line: line[0] != "ticker") \
    .filter(lambda line: "2016" <= line[7][0:4] <= "2018")

input_file_2 = input_file_2 \
    .filter(lambda line: line[0] != "ticker") \
    .filter(lambda line: line[3] != "N/A") \

# (ticker, ((close, date), name))
total = input_file \
    .map(lambda line: (line[0], (line[2], line[7]))) \
    .join(input_file_2.map(lambda line: (line[0], (line[2]))))

# (ticker, close, date, name)
total = total \
    .map(lambda line: (line[0], float(line[1][0][0]), line[1][0][1], line[1][1]))

# ((name, ticker, year), (first_date_close, close)))
first_data = total \
    .map(lambda line: ((line[3], line[0], parse_year(line[2])), (line[2], line[1]))) \
    .reduceByKey(lambda x, y: first_close_date(x, y))

# ((name, ticker, year), (last_date_close, close)))
last_data = total \
    .map(lambda line: ((line[3], line[0], parse_year(line[2])), (line[2], line[1]))) \
    .reduceByKey(lambda x, y: last_close_date(x, y))

# ((name, ticker, year), ((first_date_close, close), (last_date_close, close)))
year_change = first_data \
    .join(last_data)

# ((name, ticker, year), var)
year_change = year_change \
    .map(lambda line: (line[0], (((line[1][1][1]-line[1][0][1])/line[1][0][1]) * 100) ))

# (name, [(year_1, var), (year_2, var), (year_3, var)])
year_change = year_change \
    .map(lambda line: (line[0][0], [(line[0][2], line[1])])) \
    .reduceByKey(lambda x, y: x+y)

# ((var_1, var_2, var_3), [name])
year_change = year_change \
    .map(lambda line: ((line[1][0][1], line[1][1][1], line[1][2][1]), [line[0]]))

# ((var_1, var_2, var_3), [name_1, name_2, ...])
year_change = year_change \
    .reduceByKey(lambda x, y: x+y)


year_change.coalesce(1).saveAsTextFile("file:///Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job3/spark/result/job_3_result")




