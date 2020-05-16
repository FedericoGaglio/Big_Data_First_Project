#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pyspark import SparkConf, SparkContext, StorageLevel

conf = SparkConf().setMaster("local[*]").setAppName("Job1")
sc = SparkContext(conf=conf)


def min_close(x, y):
    if x[1] > y[1]:
        return y
    else:
        return x


def max_close(x, y):
    if x[1] > y[1]:
        return x
    else:
        return y


input_file = sc.textFile(
    "file:///Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/10000000.csv") \
    .map(lambda line: line.split(","))

input_file = input_file \
    .filter(lambda line: line[0] != "ticker")

main_table = input_file \
    .filter(lambda line: "2008" <= line[7][0:4] <= "2018")

# persist RDD in memory
main_table \
    .persist(StorageLevel.MEMORY_AND_DISK)

# (ticker, min_close)
min_ticker_close = main_table \
    .map(lambda line: (line[0], float(line[2]))) \
    .reduceByKey(lambda x, y: min(x, y))

# (ticker, max_close)
max_ticker_close = main_table \
    .map(lambda line: (line[0], float(line[2]))) \
    .reduceByKey(lambda x, y: max(x, y))

# (ticker, avg_volume)
avg_ticker_volume = main_table \
    .map(lambda line: (line[0], (float(line[6]), 1))) \
    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) \
    .map(lambda line: (line[0], (line[1][0] / line[1][1])))

# (ticker, (close, first_data))
first_data_close = main_table \
    .map(lambda line: (line[0], (line[2], line[7]))) \
    .reduceByKey(lambda x, y: min_close(x, y))

# (ticker, (close, last_data))
last_data_close = main_table \
    .map(lambda line: (line[0], (line[2], line[7]))) \
    .reduceByKey(lambda x, y: max_close(x, y))

# (ticker, ((close, first_data), (close, last_data)))
join_inc_per = first_data_close.join(last_data_close)

# (ticker, inc_per)
inc_per = join_inc_per \
    .map(lambda line: ((line[0]),
                       (((float(line[1][1][0]) - float(line[1][0][0])) / float(line[1][0][0])) * 100)))

# (ticker, (((min, max), avg), inc_prc))
result = min_ticker_close \
    .join(max_ticker_close) \
    .join(avg_ticker_volume) \
    .join(inc_per) \
    .sortBy(lambda line: line[0]) \
    .map(lambda line: {"ticker": line[0],
                       "min_price": line[1][0][0][0],
                       "max_price": line[1][0][0][1],
                       "average_volume": line[1][0][1],
                       "increase_percent": line[1][1]})

result.coalesce(1).saveAsTextFile("file:///Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/job1/spark/result/job_1_result")

