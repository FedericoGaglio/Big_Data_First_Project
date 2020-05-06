#!/usr/bin/env python

import sys
import csv

START_YEAR = 2016
END_YEAR = 2018

STOCKS_INFO_FILE = '/Users/alessio/Documents/Universita/big-data/Big_Data_First_Project/dataset/historical_stocks.csv'

def respect_condition(year, ticker):
    return year in range(START_YEAR, END_YEAR + 1) and ticker in ticker_2_name

ticker_2_name = {}

with open(STOCKS_INFO_FILE) as csv_file:
    csv_reader = csv.reader(csv_file, delimiter=',')
    firstLine = True

    for row in csv_reader:
        if not firstLine:
            ticker, _, name, _, _ = row
            if name != 'N/A':
                ticker_2_name[ticker] = name
        else:
            firstLine = False

for line in sys.stdin:
    data = line.strip().split(',')
    if len(data) == 8:
        ticker, _, close, _, _, _, _, date = data
        if ticker != 'ticker':
            year = int(date[0:4])

            if respect_condition(year, ticker):
                name = ticker_2_name[ticker]
                print('{}\t{}\t{}\t{}'.format(ticker, date, close, name))
